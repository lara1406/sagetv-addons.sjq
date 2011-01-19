/**
 * 
 */
package com.google.code.sagetvaddons.sjq.server;

import java.io.IOException;
import java.util.Date;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.google.code.sagetvaddons.sjq.network.AgentClient;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask;

/**
 * @author dbattams
 *
 */
final class ActiveTaskManager extends TimerTask {
	static private final Logger LOG = Logger.getLogger(ActiveTaskManager.class);
	
	/* (non-Javadoc)
	 * @see java.util.TimerTask#run()
	 */
	@Override
	public void run() {
		int i = 0;
		synchronized(TaskQueue.get()) {
			DataStore ds = DataStore.get();
			for(QueuedTask qt : ds.getActiveQueue()) {
				if(qt.getState() == QueuedTask.State.RUNNING || qt.getState() == QueuedTask.State.STARTED) {
					if(qt.getState() == QueuedTask.State.STARTED && System.currentTimeMillis() - qt.getStarted().getTime() < 30000) // Skip tasks that were just assigned; give the client 30 seconds to update the state
						continue;
					++i;
					AgentClient agent = null;
					try {
						agent = new AgentClient(qt.getAssignee(), Config.get().getLogPkg());
						if(!agent.isTaskActive(qt)) {
							qt.setState(QueuedTask.State.FAILED);
							qt.setCompleted(new Date());
							ds.updateTask(qt);
							LOG.warn("Marked " + qt + " as failed because " + qt.getAssignee() + " says it's not running the task!");
						}
					} catch (IOException e) {
						LOG.error("IOError", e);
					} finally {
						if(agent != null)
							agent.close();
					}
				}
			}
		}
		if(i > 0 || LOG.isDebugEnabled())
			LOG.info("Validated " + i + " running task(s)!");
	}
}
