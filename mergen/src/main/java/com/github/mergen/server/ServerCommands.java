package com.github.mergen.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.HazelcastInstance;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channels;
import java.util.*;
import java.lang.annotation.*;
import java.util.concurrent.*;

import com.github.nedis.codec.CommandArgs;

/**
 * this is controller for redis commands, dispatcher calls this
 */
public class ServerCommands extends Controller {
	private final String kvstorename = "__kvstore";
	
	/**
	 * sets the namespace, so different customers/users can easily
	 * use different databases...
	 * @param e
	 * @param args
	 */
	@RedisCommand(cmd = "DB", returns = "OK")
	public void db(MessageEvent e, Object[] args) {
		String namespace = new String((byte[]) args[1]);
		this.base.setNamespace(namespace);
		// this is not namespaced.
		IList<String> list = base.client.getClient().getList("HZ-DATABASES");
		if (!list.contains(namespace)){
			list.add(namespace);
		}
	}

	@RedisCommand(cmd = "DATABASES")
	public void databases(MessageEvent e, Object[] args) {
		IList<String> list = base.client.getClient().getList("HZ-DATABASES");
		
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

	
	
	@RedisCommand(cmd = "SET", returns = "OK")
	public void set(MessageEvent e, Object[] args) {
		String k = new String((byte[]) args[1]);
		String v = new String((byte[]) args[2]);
		IMap<String, String> kvstore = base.client.getMap(kvstorename);
		kvstore.set(k, v, 0, TimeUnit.SECONDS);
	}

	@RedisCommand(cmd = "SETEX", returns = "OK")
	public void setex(MessageEvent e, Object[] args) {
		String k = new String((byte[]) args[1]);
		String v = new String((byte[]) args[3]);
		int ttl = Integer.parseInt(new String((byte[]) args[2]));
		IMap<String, String> kvstore = base.client.getMap(kvstorename);
		kvstore.set(k, v, ttl, TimeUnit.SECONDS);
	}

	@RedisCommand(cmd = "GET")
	public void get(MessageEvent e, Object[] args) {
		String k = new String((byte[]) args[1]);

		IMap<String, String> kvstore = base.client.getMap(kvstorename);
		Object v = kvstore.get(k);

		if (v == null) {
			ServerReply sr = new ServerReply();
			e.getChannel().write(sr.replyNone());
		} else {
			CommandArgs c = new CommandArgs();
			c.add((String) v);
			e.getChannel().write(c.buffer());
		}
	}

	@RedisCommand(cmd = "DEL")
	public void del(MessageEvent e, Object[] args) {
		int delcnt = 0;
		IMap<String, String> kvstore = base.client.getMap(kvstorename);

		for (int i = 1; i < args.length; i++) {
			String k = new String((byte[]) args[i]);
			Object v = kvstore.remove(k);
			if (v != null) {
				delcnt++;
			}
		}
		ServerReply sr = new ServerReply();
		e.getChannel().write(sr.replyInt(delcnt));
	}

	@RedisCommand(cmd = "EXISTS")
	public void exists(MessageEvent e, Object[] args) {
		String k = new String((byte[]) args[1]);
		IMap<String, String> kvstore = base.client.getMap(kvstorename);
		if (kvstore.containsKey(k)) {
			ServerReply sr = new ServerReply();
			e.getChannel().write(sr.replyInt(1));
		} else {
			ServerReply sr = new ServerReply();
			e.getChannel().write(sr.replyInt(0));
		}
	}

	@RedisCommand(cmd = "KEYS")
	public void keys(MessageEvent e, Object[] args) {
		// this is half baked
		IMap<String, String> kvstore = base.client.getMap(kvstorename);
		Set<String> keys = kvstore.keySet();
		String[] array = keys.toArray(new String[0]);
		ServerReply sr = new ServerReply();
		e.getChannel().write(sr.replyMulti(array));
	}

	@RedisCommand(cmd = "PING", returns = "status", authenticate = false)
	public String ping(MessageEvent e, Object[] args) {
		return "PONG";
	}
	
	@RedisCommand(cmd = "AUTH", returns = "OK", authenticate = false)
	public void auth(MessageEvent e, Object[] args) {
		// just raise an exception if not authenticated
		this.base.authenticated = true;
	}

	@RedisCommand(cmd = "INFO")
	public void info(MessageEvent e, Object[] args) {
		ServerReply sr = new ServerReply();
		ServerReply.MultiReply mr = sr.startMultiReply();
		mr.addString("redis_version:0.0.1");
		mr.addString("mergen_version:0.0.1");		
		mr.finish();
		e.getChannel().write(mr.getBuffer());
	}
	
	@RedisCommand(cmd = "SHUTDOWN", returns="OK")
	public void shutdown(MessageEvent e, Object[] args) {
		System.exit(0);
	}
	
}