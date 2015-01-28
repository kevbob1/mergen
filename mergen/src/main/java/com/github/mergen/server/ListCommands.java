package com.github.mergen.server;

import java.util.Iterator;

import com.hazelcast.core.IList;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Transaction;

import org.jboss.netty.channel.MessageEvent;

import com.github.nedis.codec.CommandArgs;


public class ListCommands extends Controller {

	@RedisCommand(cmd = "LCLEAR", returns = "OK")
	public void clear(MessageEvent e, Object[] args) {
		String listname = new String((byte[]) args[1]);
		IList<String> list = base.client.getList(listname);
		list.clear();
	}
	
	
	@RedisCommand(cmd = "RPUSH", returns = "OK")
	public void rpush(MessageEvent e, Object[] args) {
		String listname = new String((byte[]) args[1]);
		String v = new String((byte[]) args[2]);
		IList<String> list = base.client.getList(listname);
		list.add(v);
	}

	@RedisCommand(cmd = "LPUSH", returns = "OK")
	public void lpush(MessageEvent e, Object[] args) {
		String listname = new String((byte[]) args[1]);
		String v = new String((byte[]) args[2]);
		IList<String> list = base.client.getList(listname);
		list.add(0, v);
	}
	
	@RedisCommand(cmd = "LPOP")
	public void lpop(MessageEvent e, Object[] args) {
		String listname = new String((byte[]) args[1]);
		IList<String> list = base.client.getList(listname);
		Transaction txn1 = base.client.getTransaction();
		
		String v = null;
		txn1.begin();
		try {			
			v = list.get(0);
			list.remove(0);			
		} catch (IndexOutOfBoundsException exc){
			// pass
		} finally {
			txn1.commit();
		}
		
		if (v == null) {
			ServerReply sr = new ServerReply();
			e.getChannel().write(sr.replyNone());
		} else {
			CommandArgs c = new CommandArgs();
			c.add((String) v);
			e.getChannel().write(c.buffer());
		}
	}

   @RedisCommand(cmd = "BLPOP")
   public void blpop(MessageEvent e, Object[] args) {
      long defaultTimeout = 30000;
      final long startTime = System.currentTimeMillis();
      String listname = new String((byte[]) args[1]);
      IList<String> list = base.client.getList(listname);
      Transaction txn1 = base.client.getTransaction();
      
      String v = null;
      
      ItemListener<String> listener = new ItemListener<String>() {
         
         @Override
         public void itemAdded(ItemEvent<String> event) {
            synchronized(this){
               this.notify();
            }
         }

         @Override
         public void itemRemoved(ItemEvent<String> event) {
            // do nothing we don't care
         }
         
      };
      
      try {       
         txn1.begin();
         v = list.remove(0);         
      } catch (IndexOutOfBoundsException exc){
         // pass
      } finally {
         txn1.commit();
      }
      
      if (v == null) {
      try {
         list.addItemListener(listener, false);
         while (v == null && (System.currentTimeMillis() - startTime) < defaultTimeout) {
            // loop until we get something, or time out
            synchronized (listener) {
               try {
                  listener.wait(300);
               } catch (InterruptedException e1) {
                  break;
               }
            }
               
            try {       
               txn1.begin();
               v = list.remove(0);         
               if (v != null) {
                  break;
               }
            } catch (IndexOutOfBoundsException exc){
               // pass
            } finally {
               txn1.commit();
            }
            
         }
      } finally {
         list.removeItemListener(listener);
      }
      }
      
      if (v == null) {
         ServerReply sr = new ServerReply();
         e.getChannel().write(sr.replyNone());
      } else {
         CommandArgs c = new CommandArgs();
         c.add((String) v);
         e.getChannel().write(c.buffer());
      }
   }
 
	
	
   @RedisCommand(cmd = "LLEN")
   public void llen(MessageEvent e, Object[] args) {
      String listname = new String((byte[]) args[1]);
      IList<String> list = base.client.getList(listname);
      
      int length = list.size();
      ServerReply sr = new ServerReply();
      e.getChannel().write(sr.replyInt(length));
   }
	
	
	@RedisCommand(cmd = "LGETALL")
	public void lrange(MessageEvent e, Object[] args) {
		String listname = new String((byte[]) args[1]);
		IList<String> list = base.client.getList(listname);		
		
		ServerReply sr = new ServerReply();
		ServerReply.MultiReply mr = sr.startMultiReply();

		Iterator<String> it = list.iterator();
		while (it.hasNext()) { 
		    String val = (String) it.next(); 
			if (val == null) {
				mr.addNull();
			} else {
				mr.addString(val);
			}
		}
		mr.finish();
		e.getChannel().write(mr.getBuffer());		
	}
	
	
}