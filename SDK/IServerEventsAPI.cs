using System;
using System.Collections.Generic;
using UnityEngine.Events;

namespace Nox.Servers {
	public interface IServerEventsAPI {
		IReadOnlyCollection<string> Frames { get; }

		bool IsSubscribed(string frame);

		void Subscribe(string frame);

		void Unsubscribe(string frame);

		void On(string frame, Action<SocketPacket> handler);

		void Off(string frame, Action<SocketPacket> handler);

		void On<T>(string frame, Action<SocketPacket<T>> handler);

		void Off<T>(string frame, Action<SocketPacket<T>> handler);

		UnityEvent<string> OnSubscribed { get; }

		UnityEvent<string> OnFrame { get; }
	}
}
