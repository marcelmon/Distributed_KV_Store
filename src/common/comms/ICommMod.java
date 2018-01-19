package common.comms;

import common.messages.*;

public interface ICommMod {
	public void SetListener(ICommListener listener);
	public boolean SendMessage(KVMessage msg);
}
