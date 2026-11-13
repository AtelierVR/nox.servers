using System;
using System.Collections.Generic;
using Nox.CCK.Utils;
using UnityEngine;
using Logger = Nox.CCK.Utils.Logger;

namespace Nox.Servers.Runtime.Networks {
	/// <summary>
	/// Logs the frames received on the server socket without flooding the log: a repeating frame (an
	/// asset analysis emits one <c>world.files.updated</c> every few milliseconds) is summarised by
	/// its name and throttled, because each logged line costs a console entry, a file write and a
	/// main-thread continuation.
	/// </summary>
	internal static class FrameLog {
		private const string LOG_TAG = "socket";

		/// <summary>Configuration key that enables the frame log; off by default.</summary>
		private const string ENABLED_KEY = "debug.socket.frames";

		/// <summary>Delay between two payloads logged for the same frame.</summary>
		private const float INTERVAL = 1f;

		/// <summary>Length the payload is cut to.</summary>
		private const int MAX_LENGTH = 512;

		private static readonly Dictionary<string, float> _lastLog = new();
		private static readonly Dictionary<string, int> _collapsed = new();

		private static readonly string[] _path = ENABLED_KEY.Split('.');

		private static bool? _enabled;

		static FrameLog() {
			Config.OnChanged.AddListener(OnConfigChanged);
			Config.OnReloaded.AddListener(OnConfigReloaded);
		}

		/// <summary>Whether the received frames are written to the log (<c>debug.socket.frames</c>).</summary>
		internal static bool Enabled {
			get => _enabled ??= Config.Load().Get(ENABLED_KEY, false);
			set {
				_enabled = value;

				var config = Config.Load();
				config.Set(ENABLED_KEY, value);
				config.Save();
			}
		}

		/// <summary>The config was edited: follow the flag without waiting for a reload.</summary>
		private static void OnConfigChanged(string[] path, object value, object previous) {
			if (path == null || path.Length != _path.Length)
				return;

			for (var i = 0; i < path.Length; i++)
				if (!string.Equals(path[i], _path[i], StringComparison.Ordinal))
					return;

			_enabled = value is bool enabled ? enabled : null;
		}

		/// <summary>The config was reloaded from disk (edited outside Unity): the cache is stale.</summary>
		private static void OnConfigReloaded()
			=> _enabled = null;

#if UNITY_EDITOR
		[UnityEditor.MenuItem("Nox/Logger/Toggle Socket Frames")]
		private static void ToggleFrames()
			=> Enabled = !Enabled;

		[UnityEditor.MenuItem("Nox/Logger/Toggle Socket Frames", true)]
		private static bool ToggleFramesValidate() {
			UnityEditor.Menu.SetChecked("Nox/Logger/Toggle Socket Frames", Enabled);
			return true;
		}
#endif

		internal static void Reset() {
			_lastLog.Clear();
			_collapsed.Clear();
		}

		internal static void Frame(string payload) {
			if (!Enabled || string.IsNullOrEmpty(payload))
				return;

			var key = Name(payload);
			var now = Time.realtimeSinceStartup;

			if (_lastLog.TryGetValue(key, out var last) && now - last < INTERVAL) {
				_collapsed[key] = _collapsed.TryGetValue(key, out var count) ? count + 1 : 1;
				return;
			}

			_lastLog[key] = now;

			if (_collapsed.TryGetValue(key, out var pending) && pending > 0) {
				_collapsed[key] = 0;
				Logger.LogDebug($"{key} (+{pending} collapsed): {Cut(payload)}", tag: LOG_TAG);
				return;
			}

			Logger.LogDebug($"{key}: {Cut(payload)}", tag: LOG_TAG);
		}

		/// <summary>Frame name: the <c>name</c> of a room event, else the envelope <c>type</c>.</summary>
		private static string Name(string payload)
			=> Value(payload, "\"name\":\"")
				?? Value(payload, "\"type\":\"")
				?? "frame";

		private static string Value(string payload, string prefix) {
			var start = payload.IndexOf(prefix, StringComparison.Ordinal);

			if (start < 0)
				return null;

			start += prefix.Length;

			var end = payload.IndexOf('"', start);

			return end > start
				? payload.Substring(start, end - start)
				: null;
		}

		private static string Cut(string payload)
			=> payload.Length <= MAX_LENGTH
				? payload
				: payload.Substring(0, MAX_LENGTH) + $"... ({payload.Length} bytes)";
	}
}
