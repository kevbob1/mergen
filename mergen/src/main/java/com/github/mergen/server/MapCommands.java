package com.github.mergen.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
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

import java.nio.charset.Charset;

public class MapCommands extends Controller {

	
	@RedisCommand(cmd="HSET", returns="OK")
	public void hset(MessageEvent e, Object[] args){		
		String map = new String((byte[])args[1]);
		String k = new String((byte[])args[2]);
		String v = new String((byte[])args[3]);				
		IMap<String, String> kvstore = base.client.getMap(map);
		kvstore.set(k, v, 0, TimeUnit.SECONDS);
	}

	@RedisCommand(cmd="HSETEX", returns="OK")
	public void hsetex(MessageEvent e, Object[] args){
		String map = new String((byte[])args[1]);
		
		String k = new String((byte[])args[2]);
		String v = new String((byte[])args[4]);				
		int ttl = Integer.parseInt( new String((byte[])args[3]) );

		IMap<String, String> kvstore = base.client.getMap(map);
		kvstore.set(k, v, ttl, TimeUnit.SECONDS);
	}



	@RedisCommand(cmd="HGET")
	public void get(MessageEvent e, Object[] args){		
		String map = new String((byte[])args[1]);
		String k = new String((byte[])args[2]);
				
		IMap<String, String> kvstore = base.client.getMap(map);
		Object v = kvstore.get(k);

		if (v == null){
			ServerReply sr = new ServerReply();
			e.getChannel().write(sr.replyNone());
		} else {
			CommandArgs c = new CommandArgs();
			c.add((String)v);
			e.getChannel().write(c.buffer());
		}
	}


	@RedisCommand(cmd="HMGET")
	public void hmget(MessageEvent e, Object[] args){		
		int delcnt = 0;
		String map = new String((byte[])args[1]);
		IMap<String, String> kvstore = base.client.getMap(map);				
		
		Set<String> keys = new LinkedHashSet<String>(); // we need order

		for(int i=2;i<args.length; i++) {
			String k = new String((byte[])args[i]);	
			System.out.println("adding >" + k);
			keys.add(k);
		}

		Map<String, String> r = kvstore.getAll(keys);

		ServerReply sr = new ServerReply();
		ServerReply.MultiReply mr = sr.startMultiReply();

		for (String val: keys){
			System.out.println(">>>>> " + val + " : " + r.get(val) );
			if (r.get(val) == null){
				mr.addNull();
			} else {
				mr.addString(r.get(val));				
			}
			
		}
		System.out.println(">>>>> 1");
		mr.finish();
		System.out.println(">>>>> 2");

		System.out.println(mr.getBuffer().toString(Charset.defaultCharset()));
		System.out.println(">>>>> 3");

		e.getChannel().write(mr.getBuffer());
		System.out.println(">>>>> 4");
		// ServerReply sr = new ServerReply();
		// e.getChannel().write(sr.replyMulti(array));
	}	

	@RedisCommand(cmd="HDEL")
	public void del(MessageEvent e, Object[] args){		
		int delcnt = 0;
		String map = new String((byte[])args[1]);
		IMap<String, String> kvstore = base.client.getMap(map);				

		for(int i=2;i<args.length; i++) {
			String k = new String((byte[])args[i]);	
			Object v = kvstore.remove(k);			
			if (v!=null){
				delcnt++;	
			}			
		}
		ServerReply sr = new ServerReply();
		e.getChannel().write(sr.replyInt(delcnt));
	}	



	@RedisCommand(cmd="HEXISTS")
	public void exists(MessageEvent e, Object[] args){		
		String map = new String((byte[])args[1]);
		String k = new String((byte[])args[2]);		
		IMap<String, String> kvstore = base.client.getMap(map);		
		if (kvstore.containsKey(k)){
			ServerReply sr = new ServerReply();
			e.getChannel().write(sr.replyInt(1));
		} else {
			ServerReply sr = new ServerReply();
			e.getChannel().write(sr.replyInt(0));
		}
	}

	@RedisCommand(cmd="HKEYS")
	public void hkeys(MessageEvent e, Object[] args){		
		// this is half baked
		String map = new String((byte[])args[1]);
		IMap<String, String> kvstore = base.client.getMap(map);
		Set<String> keys = kvstore.keySet();
		String[] array = keys.toArray(new String[0]);
		System.out.println(Arrays.toString(array));
		ServerReply sr = new ServerReply();
		e.getChannel().write(sr.replyMulti(array));
	}	

	@RedisCommand(cmd="HLEN")
	public void hlen(MessageEvent e, Object[] args){		
		// this is half baked
		String map = new String((byte[])args[1]);
		IMap<String, String> kvstore = base.client.getMap(map);
		ServerReply sr = new ServerReply();
		e.getChannel().write(sr.replyInt(kvstore.size()));
	}	

}