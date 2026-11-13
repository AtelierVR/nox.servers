using System;
using System.Collections.Generic;
using System.Linq;
using Cysharp.Threading.Tasks;
using Nox.CCK.Language;
using Nox.CCK.Mods.Cores;
using Nox.CCK.Mods.Events;
using Nox.CCK.Mods.Initializers;
using Nox.CCK.Utils;
using Nox.Network;
using Nox.Servers.Runtime.Networks;
using Nox.Users;
using UnityEngine.Events;

namespace Nox.Servers.Runtime {
	public class Main : IMainModInitializer, IServerAPI, IServerEventsAPI {
		internal IMainModCoreAPI CoreAPI;
		internal static Main Instance;

		private LanguagePack _lang;
		internal (Identifier, ServerSocket) Socket = (Identifier.Invalid, null);
		private readonly object _socketLock = new();
		private bool _isConnecting;
		private bool _disposed;

		private static readonly TimeSpan _socketTimeout = TimeSpan.FromSeconds(30);

		/// <summary>WebSocket subscriptions of the current user's server, kept across reconnections.</summary>
		internal readonly FrameSubscriptions Subscriptions = new();

		internal static INetworkAPI NetworkAPI
			=> Instance.CoreAPI.ModAPI
				.GetMod("network")
				?.GetInstance<INetworkAPI>();

		internal static IUserAPI UserAPI
			=> Instance.CoreAPI.ModAPI
				.GetMod("users")
				?.GetInstance<IUserAPI>();

		internal readonly UnityEvent<INoxObject> OnServerUpdated = new();
		internal readonly UnityEvent<INoxObject> OnServerFetched = new();
		internal readonly UnityEvent<INoxObject> OnServerConnected = new();
		internal readonly UnityEvent<INoxObject> OnServerDisconnected = new();
		internal readonly UnityEvent OnSocketConnected = new();
		internal readonly UnityEvent OnSocketDisconnected = new();

		UnityEvent IServerAPI.OnSocketConnected
			=> OnSocketConnected;
		UnityEvent IServerAPI.OnSocketDisconnected
			=> OnSocketDisconnected;

		private EventSubscription[] _events = Array.Empty<EventSubscription>();

		public void OnInitializeMain(IMainModCoreAPI api) {
			CoreAPI = api;
			Instance = this;
			_lang = CoreAPI.AssetAPI.GetAsset<LanguagePack>("lang.asset");
			LanguageManager.AddPack(_lang);

			_events = new[] {
				CoreAPI.EventAPI.Subscribe(
					"server_update",
					data => OnServerUpdated.Invoke(
						data.TryGet(0, out INoxObject server)
							? server
							: null
					)
				),
				CoreAPI.EventAPI.Subscribe(
					"server_fetch",
					data => OnServerFetched.Invoke(
						data.TryGet(0, out INoxObject server)
							? server
							: null
					)
				),
				CoreAPI.EventAPI.Subscribe(
					"server_connect",
					data => OnServerConnected.Invoke(
						data.TryGet(0, out INoxObject server)
							? server
							: null
					)
				),
				CoreAPI.EventAPI.Subscribe(
					"server_disconnect",
					data => OnServerDisconnected.Invoke(
						data.TryGet(0, out INoxObject server)
							? server
							: null
					)
				),
				CoreAPI.EventAPI.Subscribe("user_update", OnCurrentUserUpdated)
			};
		}

		private void OnCurrentUserUpdated(EventData context) {
			var user = context.TryGet(0, out IUser u) ? u : null;
			StartCurrentSocket(user).Forget();
		}


		public async UniTask OnPostInitializeMainAsync() {
			var user = UserAPI.Current ?? await UserAPI.FetchCurrent();

			StartCurrentSocket(user).Forget();
		}

		private async UniTask StartCurrentSocket(IUser user) {
			// Prévenir les connexions concurrentes
			lock (_socketLock) {
				if (_isConnecting) {
					Logger.LogDebug("Socket connection already in progress, skipping.");
					return;
				}
				_isConnecting = true;
			}

			try {
				if (Socket.Item2 != null && Socket.Item1.IsValid() && Socket.Item1.Equals(user?.Identifier)) {
					Logger.LogDebug("Already connected to server for current user.");
					return;
				}

				if (Socket.Item2 != null) {
					Subscriptions.Detach(Socket.Item2);
					await Socket.Item2.Dispose();
				}

				if (user == null) {
					Logger.LogDebug("No current user, not connecting to server.");
					Socket = (Identifier.Invalid, null);
					return;
				}

				Socket = (user.Identifier, null);

				var address = user.Server;
				if (address == null) {
					Logger.LogWarning("Current user has no server address set, cannot connect to server.");
					return;
				}

				var token = await UserAPI.GetToken(address);

				var socket = await ServerSocket.Make(address, token)
					.Timeout(_socketTimeout, DelayType.Realtime);
				if (socket == null) {
					Logger.LogError($"Failed to connect to server at {address}");
					return;
				}

				Socket = (user.Identifier, socket);

				socket.OnMessageReceived.AddListener(FrameLog.Frame);
				socket.OnError.AddListener(Instance.CoreAPI.LoggerAPI.LogException);
				socket.OnConnected.AddListener(() => {
					Logger.LogDebug("Connected to server");
					FrameLog.Reset();
					Subscriptions.Attach(socket);
					CoreAPI.EventAPI.Emit("socket_connect");
					OnSocketConnected.Invoke();
				});
				socket.OnDisconnected.AddListener(() => {
					Logger.LogDebug("Disconnected from server");
					Subscriptions.Detach(socket);
					CoreAPI.EventAPI.Emit("socket_disconnect");
					OnSocketDisconnected.Invoke();
				});

				if (await socket.Connect().Timeout(_socketTimeout, DelayType.Realtime))
					return;

				Logger.LogWarning($"Server at {address} did not answer, releasing the socket.");
				Socket = (user.Identifier, null);
				Subscriptions.Detach(socket);
				await socket.Dispose();
			} catch (TimeoutException) {
				Logger.LogWarning($"Connecting to the server timed out after {_socketTimeout.TotalSeconds:0} s.");
			} catch (OperationCanceledException) {
				// The attempt was cancelled (mod unload): nothing to report.
			} catch (Exception e) {
				Logger.LogError(e);
			} finally {
				lock (_socketLock)
					_isConnecting = false;
			}
		}

		public async UniTask OnDisposeMainAsync() {
			if (_disposed)
				return;
			_disposed = true;

			foreach (var ev in _events.Where(e => e != null))
				CoreAPI?.EventAPI.Unsubscribe(ev);
			LanguageManager.RemovePack(_lang);
			_lang = null;

			if (Socket.Item2 != null) {
				Subscriptions.Detach(Socket.Item2);
				await Socket.Item2.Dispose();
			}

			Socket = (Identifier.Invalid, null);

			CoreAPI = null;
			Instance = null;
		}

		public async UniTask<IServer> Fetch(string from = null)
			=> await Networks.Network.Fetch(from);

		public async UniTask<IServerSocket> Connect(string address)
			=> await ServerSocket.Make(address);

		public IServerSocket Current
			=> Socket.Item2;

		// ── Persistent WebSocket subscriptions ───────────────────────────────────────────

		public IReadOnlyCollection<string> Frames
			=> Subscriptions.Frames;

		public UnityEvent<string> OnSubscribed
			=> Subscriptions.OnSubscribed;

		public UnityEvent<string> OnFrame
			=> Subscriptions.OnFrame;

		public bool IsSubscribed(string frame)
			=> Subscriptions.IsSubscribed(frame);

		public void Subscribe(string frame)
			=> Subscriptions.Subscribe(frame);

		public void Unsubscribe(string frame)
			=> Subscriptions.Unsubscribe(frame);

		public void On(string frame, Action<SocketPacket> handler)
			=> Subscriptions.On(frame, handler);

		public void Off(string frame, Action<SocketPacket> handler)
			=> Subscriptions.Off(frame, handler);

		void IServerEventsAPI.On<T>(string frame, Action<SocketPacket<T>> handler)
			=> Subscriptions.On(frame, handler);

		void IServerEventsAPI.Off<T>(string frame, Action<SocketPacket<T>> handler)
			=> Subscriptions.Off(frame, handler);

		// ── Packet helpers (delegate to Current socket) ────────────────────────────────────

		public UniTask Emit<T>(SocketPacket<T> packet)
			=> Current?.Emit(packet) ?? UniTask.CompletedTask;

		public void On<T>(string type, Action<SocketPacket<T>> handler)
			=> Current?.On(type, handler);

		public void Off<T>(string type, Action<SocketPacket<T>> handler)
			=> Current?.Off(type, handler);

		public void Once<T>(string type, Action<SocketPacket<T>> handler)
			=> Current?.Once(type, handler);
	}
}
