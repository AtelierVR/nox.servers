using System;
using System.Collections.Generic;
using Cysharp.Threading.Tasks;
using Newtonsoft.Json;
using UnityEngine.Events;
using Logger = Nox.CCK.Utils.Logger;

namespace Nox.Servers.Runtime.Networks {
	internal sealed class FrameSubscriptions : IServerEventsAPI {
		public const string LOG_TAG = "ServerEvents";

		private const string ENVELOPE = "event";

		private static readonly HashSet<string> _control = new(StringComparer.Ordinal) {
			"hello",
			"error",
			"pong",
			"ping",
			"subscribed",
			"unsubscribed"
		};

		private readonly object _gate = new();
		private readonly Dictionary<string, int> _counts = new(StringComparer.Ordinal);
		private readonly List<string> _frames = new();
		private readonly Dictionary<string, List<Action<SocketPacket>>> _handlers = new(StringComparer.Ordinal);
		private readonly Dictionary<object, Action<SocketPacket>> _wrappers = new();

		private IServerSocket _socket;
		private bool _synced;

		public UnityEvent<string> OnSubscribed { get; } = new UnityEvent<string>();
		public UnityEvent<string> OnFrame { get; } = new UnityEvent<string>();

		public IReadOnlyCollection<string> Frames {
			get {
				lock (_gate)
					return _frames.ToArray();
			}
		}

		public bool IsSubscribed(string frame) {
			if (string.IsNullOrEmpty(frame))
				return false;

			lock (_gate)
				return _counts.ContainsKey(frame);
		}

		public void Subscribe(string frame) {
			if (string.IsNullOrEmpty(frame))
				return;

			lock (_gate) {
				if (_counts.TryGetValue(frame, out var count)) {
					_counts[frame] = count + 1;
					return;
				}

				_counts[frame] = 1;
				_frames.Add(frame);
			}

			Emit("subscribe", new[] { frame });
		}

		public void Unsubscribe(string frame) {
			if (string.IsNullOrEmpty(frame))
				return;

			lock (_gate) {
				if (!_counts.TryGetValue(frame, out var count))
					return;

				if (count > 1) {
					_counts[frame] = count - 1;
					return;
				}

				_counts.Remove(frame);
				_frames.Remove(frame);
			}

			Emit("unsubscribe", new[] { frame });
		}

		public void On(string frame, Action<SocketPacket> handler) {
			if (string.IsNullOrEmpty(frame) || handler == null)
				return;

			lock (_gate) {
				if (!_handlers.TryGetValue(frame, out var list))
					_handlers[frame] = list = new List<Action<SocketPacket>>();

				list.Add(handler);
			}
		}

		public void Off(string frame, Action<SocketPacket> handler) {
			if (string.IsNullOrEmpty(frame) || handler == null)
				return;

			lock (_gate) {
				if (!_handlers.TryGetValue(frame, out var list))
					return;

				list.Remove(handler);

				if (list.Count == 0)
					_handlers.Remove(frame);
			}
		}

		public void On<T>(string frame, Action<SocketPacket<T>> handler) {
			if (handler == null)
				return;

			Action<SocketPacket> wrapper = raw => handler(ServerSocket.ToTyped<T>(raw));

			lock (_gate)
				_wrappers[handler] = wrapper;

			On(frame, wrapper);
		}

		public void Off<T>(string frame, Action<SocketPacket<T>> handler) {
			Action<SocketPacket> wrapper;

			lock (_gate) {
				if (!_wrappers.TryGetValue(handler, out wrapper))
					return;

				_wrappers.Remove(handler);
			}

			Off(frame, wrapper);
		}

		/* ------------------------------------------------------------------ */
		/* Socket lifetime                                                      */
		/* ------------------------------------------------------------------ */

		public void Attach(IServerSocket socket) {
			if (socket == null)
				return;

			if (ReferenceEquals(_socket, socket)) {
				_synced = false;
				Flush();
				return;
			}

			_socket = socket;
			_synced = false;

			socket.OnPacket.AddListener(OnPacket);
			WaitHello(socket).Forget();
		}

		public void Detach(IServerSocket socket) {
			if (socket == null || !ReferenceEquals(_socket, socket))
				return;

			socket.OnPacket.RemoveListener(OnPacket);
			_socket = null;
			_synced = false;
		}

		/// <summary>Sends the whole subscription list on the current socket, if any.</summary>
		public void Flush() {
			var socket = _socket;

			if (socket == null || !socket.IsConnected())
				return;

			string[] frames;

			lock (_gate)
				frames = _frames.ToArray();

			_synced = true;

			if (frames.Length > 0)
				Emit(socket, "subscribe", frames);
		}

		private async UniTaskVoid WaitHello(IServerSocket socket) {
			await UniTask.Delay(TimeSpan.FromSeconds(1), ignoreTimeScale: true).SuppressCancellationThrow();

			if (!ReferenceEquals(_socket, socket) || _synced)
				return;

			Flush();
		}

		private void Emit(string type, string[] frames) {
			var socket = _socket;

			if (!_synced || socket == null || !socket.IsConnected())
				return;

			Emit(socket, type, frames);
		}

		private static void Emit(IServerSocket socket, string type, string[] frames)
			=> socket.Emit(
				new SocketPacket<SubscriptionPayload> {
					type = type,
					payload = new SubscriptionPayload { events = frames }
				}
			).Forget();

		/* ------------------------------------------------------------------ */
		/* Reception                                                            */
		/* ------------------------------------------------------------------ */

		private void OnPacket(SocketPacket packet) {
			var type = packet?.type;

			switch (type) {
				case null:
					return;
				case ENVELOPE:
					Unwrap(packet);
					return;
				case "hello":
					Flush();
					return;
				case "subscribed":
					Acknowledge(packet);
					return;
				case "unsubscribed":
					Rejected(packet);
					return;
				case "error":
					Refused(packet);
					return;
				case "pong":
				case "ping":
					return;
			}

			if (_control.Contains(type))
				return;

			Dispatch(type, (object)packet.payload);
		}

		private void Unwrap(SocketPacket packet) {
			var envelope = ServerSocket.ToTyped<FrameEnvelope>(packet)?.payload;

			if (envelope == null || string.IsNullOrEmpty(envelope.name))
				return;

			Dispatch(envelope.name, envelope.data);
		}

		private void Acknowledge(SocketPacket packet) {
			_synced = true;

			var ack = ServerSocket.ToTyped<SubscribedFrame>(packet)?.payload;

			if (ack?.subscribed == null)
				return;

			foreach (var frame in ack.subscribed) {
				if (string.IsNullOrEmpty(frame))
					continue;

				OnSubscribed.Invoke(frame);
			}
		}

		private static void Rejected(SocketPacket packet) {
			var ack = ServerSocket.ToTyped<UnsubscribedFrame>(packet)?.payload;

			if (ack?.unsubscribed == null)
				return;

			foreach (var frame in ack.unsubscribed)
				if (!string.IsNullOrEmpty(frame))
					Logger.LogDebug($"The server did not have '{frame}' subscribed.", tag: LOG_TAG);
		}

		private static void Refused(SocketPacket packet) {
			var error = ServerSocket.ToTyped<ErrorFrame>(packet)?.payload;

			Logger.LogWarning(
				error == null
					? "The server refused a frame."
					: $"The server refused a frame: {error.message} ({error.code}).",
				tag: LOG_TAG
			);
		}

		private void Dispatch(string frame, object payload) {
			if (string.IsNullOrEmpty(frame))
				return;

			List<Action<SocketPacket>> snapshot;

			lock (_gate) {
				snapshot = _handlers.TryGetValue(frame, out var list) && list.Count > 0
					? new List<Action<SocketPacket>>(list)
					: null;
			}

			OnFrame.Invoke(frame);

			if (snapshot == null)
				return;

			var packet = new SocketPacket { type = frame, payload = payload };

			foreach (var handler in snapshot)
				try {
					handler(packet);
				} catch (Exception e) {
					Logger.LogError(e);
				}
		}

		[Serializable]
		private class SubscriptionPayload {
			[JsonProperty("events")]
			public string[] events;
		}

		[Serializable]
		private class FrameEnvelope {
			[JsonProperty("name")]
			public string name;

			[JsonProperty("data")]
			public object data;
		}

		[Serializable]
		private class SubscribedFrame {
			[JsonProperty("subscribed")]
			public string[] subscribed;
		}

		[Serializable]
		private class UnsubscribedFrame {
			[JsonProperty("unsubscribed")]
			public string[] unsubscribed;
		}

		[Serializable]
		private class ErrorFrame {
			[JsonProperty("code")]
			public string code;

			[JsonProperty("message")]
			public string message;
		}
	}
}
