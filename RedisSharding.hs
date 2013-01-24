{-# LANGUAGE OverloadedStrings, BangPatterns  #-}

module RedisSharding (
	client_reader, servers_reader, client_sender, servers_sender
) where


import Control.Monad (when)
import Data.Int (Int64)
import Data.Digest.CRC32 (crc32)
import Data.Maybe (fromJust)
import System.IO (stderr)

import qualified Data.List as L

import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B

import Network.Socket (Socket)
import Network.Socket.ByteString (recv, sendAll)

import Data.Attoparsec

import Control.Concurrent.MVar (MVar, putMVar, takeMVar)

import RedisParser

import qualified MyListBuf as LB


data Msg = MsgFlush | MsgData !LB.ListBuf | MsgDataTo {-# UNPACK #-} !Int !LB.ListBuf

data RCmd = RCmd !ByteString ![Int] -- ��� ������� � �� ����� ������� ������� ���������� ������ 


warn = B.hPutStrLn stderr . B.concat

showInt :: Int64 -> ByteString
showInt a = B.pack $ show a


send_size = 32 * 1024
recv_size = 32 * 1024

key2server key servers = i + 1
	where
		i = fromIntegral $ (toInteger $ crc32 $ key_tag key) `rem` (toInteger $ length servers)

		key_tag ""  = ""
		key_tag key =
			case B.last key == '}' && clams /= [] of
				True  -> B.drop (1 + last clams) $ B.take (B.length key - 1) key
				False -> key
			where
				clams = B.findIndices (=='{') key


cmd_type =
	init_cmd_type 1 "PING AUTH SELECT FLUSHDB FLUSHALL DBSIZE KEYS" ++
	init_cmd_type 2 "EXISTS TYPE EXPIRE PERSIST TTL MOVE SET GET GETSET SETNX SETEX INCR INCRBY DECR DECRBY APPEND SUBSTR RPUSH LPUSH LLEN LRANGE LTRIM LINDEX LSET LREM LPOP RPOP SADD SREM SPOP SCARD SISMEMBER SMEMBERS SRANDMEMBER ZADD ZREM ZINCRBY ZRANK ZREVRANK ZRANGE ZREVRANGE ZRANGEBYSCORE ZCOUNT ZCARD ZSCORE ZREMRANGEBYRANK ZREMRANGEBYSCORE HSET HGET HMGET HMSET HINCRBY HEXISTS HDEL HLEN HKEYS HVALS HGETALL" ++
	init_cmd_type 3 "DEL MGET" ++
	init_cmd_type 4 "MSET MSETNX" ++
	init_cmd_type 5 "BLPOP BRPOP"
	where
		init_cmd_type t s = map (\a -> (a, t)) $ filter (/= "") $ B.split ' ' s



-- client_reader :: Socket
-- 	-> [ByteString]
-- 	-> [(ByteString, Socket)]
-- 	-> ((ByteString, [ByteString]) -> IO ())
-- 	-> MVar a
-- 	-> MVar b
-- 	-> IO ()
-- 	-> IO ()

client_reader c_sock servers addr2s set_cmd toServersMVar toClientMVar fquit = parseWithNext multi_bulk_parser "" c_recv >>= rf
	-- ToDo ����� ������� addr2s
	-- ToDo servers - ��� ��� ������ ��� servers_count
	where

		c_recv = recv c_sock recv_size
		c_send s = sendAll c_sock $ B.concat s
		s_send s_addr s = putMVar toServersMVar (MsgDataTo s_addr s)

		rf (Left  ("", e)) = fquit
		rf (Left  (t, e))  = c_send ["-ERR unified protocol error\r\n"]
		rf (Right (t, r))  = case r of
			Just as@((Just cmd):args) -> do
				case lookup cmd cmd_type of
					Just 1 -> do -- �� ��� �������
						set_cmd (RCmd cmd [])
						let cs = cmd2stream as
						s_send 0 cs
					Just 2 -> do -- �� ���������� ������
						let (Just key):_ = args
						let s_addr = key2server key servers
						set_cmd (RCmd cmd [s_addr])
						let cs = cmd2stream as
						s_send s_addr cs
					Just 3 -> do -- �� ��������� ��������. CMD key1 key2 ... keyN
						let arg_and_s_addr = map (\arg -> (arg, key2server (fromJust arg) servers)) args
						let s_addrs = map snd arg_and_s_addr
						let uniq_s_addrs = L.nub s_addrs
						set_cmd (RCmd cmd s_addrs)
						mapM_ (\s_addr -> do
								let _args = map fst $ filter ( \(arg, _s_addr) -> _s_addr == s_addr ) arg_and_s_addr
								let cs = cmd2stream $ concat [[Just cmd],_args]
								s_send s_addr cs
							) uniq_s_addrs
					Just 4 -> do -- �� ��������� ��������. CMD key1 value1 key2 value2 ... keyN valueN
						let arg_and_s_addr = map (\(k, v) -> ((k, v), key2server (fromJust k) servers)) $ to_pair args
						let s_addrs = map snd arg_and_s_addr
						let uniq_s_addrs = L.nub s_addrs
						set_cmd (RCmd cmd s_addrs)
						mapM_ (\s_addr -> do
								let _args = concat $ map (\((k,v),_)-> [k,v]) $
									filter ( \(arg, _s_addr) -> _s_addr == s_addr ) arg_and_s_addr
								let cs = cmd2stream $ concat [[Just cmd],_args]
								s_send s_addr cs
							) uniq_s_addrs
						where
							to_pair []      = []
							to_pair (a:b:l) = (a,b):to_pair l
					Just 5 -> do -- �� ��������� ��������. CMD key1 key2 ... keyN timeout (����������� �������)
						let timeout = last args
						let arg_and_s_addr = map (\arg -> (arg, key2server (fromJust arg) servers)) $ init args
						let s_addrs = map snd arg_and_s_addr
						let uniq_s_addrs = L.nub s_addrs
						case length uniq_s_addrs == 1 of
							False -> c_send ["-ERR Keys of the '", cmd, "' command should be on one node; use key tags\r\n"]
							True  -> do
								set_cmd (RCmd cmd s_addrs)
								mapM_ (\s_addr -> do
										let _args = map fst $ filter ( \(arg, _s_addr) -> _s_addr == s_addr ) arg_and_s_addr
										let cs = cmd2stream $ concat [[Just cmd],_args,[timeout]]
										s_send s_addr cs
									) uniq_s_addrs
					Nothing -> do
						c_send ["-ERR unsupported command '", cmd, "'\r\n"]

				when (t == "") $ putMVar toServersMVar MsgFlush

				parseWithNext multi_bulk_parser t c_recv >>= rf




servers_sender addr2s toServersMVar toClientMVar fquit = go resp_empty
	where
	go resp = do
		msg <- takeMVar toServersMVar
		case msg of
			MsgFlush           -> rbuf_send resp >> go resp_empty
			MsgDataTo s_addr r -> do
				resp <- rbuf_add resp s_addr r
				go resp

	resp_empty = map ( \ (s_addr, s_sock) -> (s_addr, s_sock, LB.empty) ) addr2s

	s_send s_sock resp = sendAll s_sock s
		where s = LB.toByteString resp

	rbuf_send resp = mapM_ go resp
		where
		go (s_addr, s_sock, r) = do
			case LB.null r of
				True  -> return ()
				False -> s_send s_sock r

	rbuf_add resp s_addr r = mapM go resp
		where
		go (_s_addr, _s_sock, _r) | s_addr == _s_addr || s_addr == 0 = do
			let r' = LB.append _r r
			case LB.length r' > send_size of
				True  -> do
					s_send _s_sock r'
					return (_s_addr, _s_sock, LB.empty)
				False -> return (_s_addr, _s_sock, r')
			-- ToDo send, ���� ������. � �����  ���� ����� ����, �� �� �������� ���������� � ��������, � ����� �������� ��� ���������.
		go x = return x




-- servers_reader :: Socket -> [(ByteString, Socket)] -> IO (ByteString, [ByteString]) -> MVar a -> MVar b -> IO () -> IO ()
servers_reader c_sock addr2s get_cmd toServersMVar toClientMVar fquit = servers_loop sss
	where
	sss = map ( \(s_addr, s_sock) -> let s_recv = recv s_sock recv_size in (s_addr, s_sock, s_recv, "") ) addr2s
	servers_loop sss = server_responses get_cmd sss toClientMVar fquit >>= servers_loop



client_sender c_sock toServersMVar toClientMVar fquit = go LB.empty
	where
	c_send resp = sendAll c_sock s
		where s = LB.toByteString resp

	go resp_old = do
		msg <- takeMVar toClientMVar
		case msg of
			MsgFlush -> c_send resp_old >> go LB.empty
			MsgData resp_new -> do
				let resp = LB.append resp_old resp_new
				case LB.length resp > send_size of
					True  -> c_send resp >> go LB.empty
					False -> go resp



server_responses get_cmd sss toClientMVar fquit = do
	RCmd cmd ss <- get_cmd
	(sss, rs) <- read_responses cmd ss sss
	sss <- join_responses cmd ss sss rs
	let ql = sum $ map (\(_,_,_,s) -> B.length s) sss
	when (ql == 0) $ putMVar toClientMVar MsgFlush
	return sss

	where

		c_send s = putMVar toClientMVar (MsgData s)

		read_responses cmd ss sss = _read_loop sss [] []
			where
				_read_loop []                                     new_sss rs = return (new_sss, rs)
				_read_loop ((s_addr, s_sock, s_recv, pr):old_sss) new_sss rs =
					case ss == [] || elem s_addr ss of
						True ->
							parseWithNext server_parser pr s_recv >>= rf
								where
								rf (Left  ("", e)) = fquit >> return (new_sss, rs)
								rf (Left  (t, e))  = c_send (LB.pack ["-ERR unified protocol error\r\n"]) >> fquit >> return (new_sss, rs)
								rf (Right (t, r))  = _read_loop old_sss ((s_addr, s_sock, s_recv, t):new_sss) ((s_addr,r):rs)

						False ->    _read_loop old_sss ((s_addr, s_sock, s_recv, pr):new_sss) rs

		join_responses cmd ss sss rs = do
			let ((_,fr):_) = rs
			case fr of
				RInt fr -> do
					-- �������� ����� ����������.
					let sm = sum $ map (\(RInt r) -> r) (map snd rs)
					c_send $ LB.pack [":", showInt sm, "\r\n"]
					return sss

				RInline fr -> do
					case any (== fr) $ map ( \(RInline r) -> r) (map snd rs) of
						True  -> c_send $ LB.pack [fr, "\r\n"] -- ������ ���������.
						False -> c_send $ LB.pack ["-ERR nodes return different results\r\n"] -- ������ ����������.
					return sss

				RBulk fmr -> do
					-- ������� ��� ��� ������� ������ ���� � ������ �������.
					let (Just ctype) = lookup cmd cmd_type
					case ctype == 2 of
						False -> warn ["bulk cmd ", cmd, " with ", showInt ctype, " != 2"]
						True  -> case length rs == 1 of
							False -> warn ["logic error"]
							True  -> c_send $ arg2stream fmr
					return sss

				RMultiSize fmrs | length rs == 1 && fmrs == -1 -> c_send (LB.pack ["*-1\r\n"]) >> return sss
				RMultiSize fmrs -> do
							c_send $ LB.pack ["*", showInt sm, "\r\n"]
							case sm > 0 of
								False -> return sss
								True  -> case length ss of
									0         -> read_loop sss $ spiral rs -- �� ���� ��� ���
									1         -> read_loop sss $ spiral rs -- � ����� ���� ���
									otherwise -> read_loop sss ss          -- � ������� ���������� ��� �� ������

							where
								sm = sum $ map (\(RMultiSize r) -> r) (map snd rs)

								-- �������, �� ������ � ������� � ��� �� ����� (������). �� ������ ���������.
								-- print $ take 5 $ spiral [ ("a", 3), ("b", 4), ("c", 2), ("d", 0) ]
								spiral a = go a []
									where
										go [] []  = []
										go [] new = go new []
										go ((k,RMultiSize v):t) new
											| v == 0    =     go t new
											| otherwise = k : go t ((k, RMultiSize(v-1)):new)

								read_loop sss ss = go sss [] ss
									where
										go sss                           []       []   = return sss
										go []                            new_sss (h:t) = go new_sss [] t 
										go ((s_addr, s_sock, s_recv, pr):old_sss) new_sss (h:t) 
											| s_addr == h = parseWithNext server_parser_multi pr s_recv >>= rf
											| otherwise = go old_sss ((s_addr, s_sock, s_recv, pr):new_sss) (h:t) 
													where
													rf (Left  ("", e)) = warn ["Parsing error server response (", cmd, ")"] >> fquit >> return sss
													rf (Left  (pr, e)) = warn ["Parsing error server response (", cmd, ")"] >> fquit >> return sss
													rf (Right (pr, r)) = case r of
														RBulk r -> do
															c_send $ arg2stream r
															go old_sss ((s_addr, s_sock, s_recv, pr):new_sss) (h:t)
