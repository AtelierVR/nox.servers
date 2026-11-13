using System;
using System.Threading;
using Cysharp.Threading.Tasks;
using Nox.CCK.Network;
using Nox.CCK.Utils;
using Nox.Servers.Runtime.Base;

namespace Nox.Servers.Runtime.Networks {
	public static class Network {
		public static readonly TimeSpan FetchTimeout = TimeSpan.FromSeconds(10);

		private static void InvokeFetch(Server server) {
			if (server == null)
				return;
			Main.Instance.CoreAPI.EventAPI.Emit("server_fetch", server);

			var config = Config.Load();
			if (!config.Has(new[] { "servers", server.Address }))
				return;

			// Gateway URL is already cached by NodeDiscover; only persist metadata here
			config.Set(new[] { "servers", server.Address, "title" }, server.Metadata?.Title);
			config.Set(new[] { "servers", server.Address, "features" }, server.Features);

			config.Save();
		}

		public static async UniTask<Server> Fetch(string address, CancellationToken cancellationToken = default) {
			if (Main.NetworkAPI == null || string.IsNullOrEmpty(address))
				return null;

			using (var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)) {
				timeout.CancelAfter(FetchTimeout);

				NoxWellKnown wk;
				try {
					// Reuse the NoxWellKnown already fetched and cached during gateway discovery
					wk = await NodeDiscover.GetWellKnown(address, timeout.Token);
				} catch (OperationCanceledException) {
					if (cancellationToken.IsCancellationRequested)
						throw;

					Logger.LogWarning($"Fetching {address} timed out after {FetchTimeout.TotalSeconds:0.#} s, the server is not answering.");
					return null;
				}

				if (wk == null) {
					Logger.LogError($"Failed to fetch server info for {address}: discovery returned no well-known document");
					return null;
				}

				var server = Server.From(wk);
				InvokeFetch(server);

				return server;
			}
		}
	}
}