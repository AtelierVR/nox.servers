using System;
using Nox.CCK.Network;

namespace Nox.Servers.Runtime.Base {
	public class ServerSoftware : IServerSoftware {
		public string Name { get; private set; }
		public Version Version { get; private set; }

		public static ServerSoftware From(NoxSoftware s)
			=> s != null
				? new ServerSoftware {
					Name    = s.name,
					Version = new Version(s.version)
				}
				: null;
	}
}