{-# LANGUAGE OverloadedStrings #-}

module RedisSharding (
	client_reader, servers_reader
) where


import Control.Monad (forM_)
import Data.Int (Int64)
import Data.Digest.CRC32 (crc32)
import Data.Maybe (fromJust)
import System.IO (stderr)

import qualified Data.List as L
import qualified Data.ByteString.Char8 as BS

import Data.Attoparsec

import RedisParser

warn = BS.hPutStrLn stderr . BS.concat

showInt :: Int64 -> BS.ByteString
showInt a = BS.pack $ show a


key2server key servers = servers !! i
	where
		i = fromIntegral $ (toInteger $ crc32 $ key_tag key) `rem` (toInteger $ length servers)

		key_tag ""  = ""
		key_tag key =
			case BS.last key == '}' && clams /= [] of
				True  -> BS.drop (1 + last clams) $ BS.take (BS.length key - 1) key
				False -> key
			where
				clams = BS.findIndices (=='{') key


cmd_type =
	init_cmd_type 1 "PING AUTH SELECT FLUSHDB FLUSHALL DBSIZE KEYS" ++
	init_cmd_type 2 "EXISTS TYPE EXPIRE PERSIST TTL MOVE SET GET GETSET SETNX SETEX INCR INCRBY DECR DECRBY APPEND SUBSTR RPUSH LPUSH LLEN LRANGE LTRIM LINDEX LSET LREM LPOP RPOP SADD SREM SPOP SCARD SISMEMBER SMEMBERS SRANDMEMBER ZADD ZREM ZINCRBY ZRANK ZREVRANK ZRANGE ZREVRANGE ZRANGEBYSCORE ZCOUNT ZCARD ZSCORE ZREMRANGEBYRANK ZREMRANGEBYSCORE HSET HGET HMGET HMSET HINCRBY HEXISTS HDEL HLEN HKEYS HVALS HGETALL" ++
	init_cmd_type 3 "DEL MGET" ++
	init_cmd_type 4 "MSET MSETNX" ++
	init_cmd_type 5 "BLPOP BRPOP"
	where
		init_cmd_type t s = map (\a -> (a, t)) $ filter (/= "") $ BS.split ' ' s


client_reader c_recv c_send servers s_send set_cmd fquit = parseWithNext multi_bulk_parser "" c_recv >>= rf
	where
		rf (Left  ("", e)) = fquit
		rf (Left  (t, e))  = c_send ["-ERR unified protocol error\r\n"]
		rf (Right (t, r))  = case r of
			Just as@((Just cmd):args) -> do
				case lookup cmd cmd_type of
					Just 1 -> do -- �� ��� �������
						set_cmd (cmd, [])
						let cs = cmd2stream as
						forM_ servers (\s_addr -> s_send s_addr cs)
					Just 2 -> do -- �� ���������� ������
						let (Just key):_ = args
						let s_addr = key2server key servers
						set_cmd (cmd, [s_addr])
						let cs = cmd2stream as
						s_send s_addr cs
					Just 3 -> do -- �� ��������� ��������. CMD key1 key2 ... keyN
						let arg_and_s_addr = map (\arg -> (arg, key2server (fromJust arg) servers)) args
						let s_addrs = map snd arg_and_s_addr
						let uniq_s_addrs = L.nub s_addrs
						set_cmd (cmd, s_addrs)
						mapM_ (\s_addr -> do
								let _args = map fst $ filter ( \(arg, _s_addr) -> _s_addr == s_addr ) arg_and_s_addr
								let cs = cmd2stream $ concat [[Just cmd],_args]
								s_send s_addr cs
							) uniq_s_addrs
					Just 4 -> do -- �� ��������� ��������. CMD key1 value1 key2 value2 ... keyN valueN
						let arg_and_s_addr = map (\(k, v) -> ((k, v), key2server (fromJust k) servers)) $ to_pair args
						let s_addrs = map snd arg_and_s_addr
						let uniq_s_addrs = L.nub s_addrs
						set_cmd (cmd, s_addrs)
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
								set_cmd (cmd, s_addrs)
								mapM_ (\s_addr -> do
										let _args = map fst $ filter ( \(arg, _s_addr) -> _s_addr == s_addr ) arg_and_s_addr
										let cs = cmd2stream $ concat [[Just cmd],_args,[timeout]]
										s_send s_addr cs
									) uniq_s_addrs
					Nothing -> do
						c_send ["-ERR unsupported command '", cmd, "'\r\n"]
				parseWithNext multi_bulk_parser t c_recv >>= rf




servers_reader c_send sss get_cmd fquit = do
	servers_loop $ map ( \(s_addr, s_sock, s_recv) -> (s_addr, s_sock, s_recv, "") ) sss
	where
	servers_loop sss = server_responses get_cmd sss c_send fquit >>= servers_loop


server_responses get_cmd sss c_send fquit = do
	(cmd, ss) <- get_cmd
	(sss, rs) <- read_responses cmd ss sss
	join_responses cmd ss sss rs -- return sss
	where
		read_responses cmd ss sss = _read_loop sss [] []
			where
				_read_loop []                                        new_sss rs = return (new_sss, rs)
				_read_loop ((s_addr, s_sock, s_recv, pr):old_sss) new_sss rs =
					case ss == [] || elem s_addr ss of
						True ->
							parseWithNext server_parser pr s_recv >>= rf
								where
								rf (Left  ("", e)) = fquit >> _read_loop old_sss ((s_addr, s_sock, s_recv, pr):new_sss) rs
								rf (Left  (t, e))  = c_send ["-ERR unified protocol error\r\n"] >> fquit >> _read_loop old_sss ((s_addr, s_sock, s_recv, pr):new_sss) rs -- ToDo
								rf (Right (t, r))  = _read_loop old_sss ((s_addr, s_sock, s_recv, t):new_sss) ((s_addr,r):rs)

						False ->    _read_loop old_sss ((s_addr, s_sock, s_recv, pr):new_sss) rs

		join_responses cmd ss sss rs = do
			let ((_,fr):_) = rs
			case fr of
				RInt fr -> do
					-- �������� ����� ����������.
					let sm = sum $ map (\(RInt r) -> r) (map snd rs)
					c_send [":", showInt sm, "\r\n"]
					return sss

				RInline fr -> do
					case any (== fr) $ map ( \(RInline r) -> r) (map snd rs) of
						True  -> c_send [fr, "\r\n"] -- ������ ���������.
						False -> c_send ["-ERR nodes return different results\r\n"] -- ������ ����������.
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

				RMultiSize fmrs | length rs == 1 && fmrs == -1 -> c_send ["*-1\r\n"] >> return sss
				RMultiSize fmrs -> do
							case sm > 0 of
								False -> c_send resp >> return sss
								True  -> case length ss of
									0         -> read_loop resp sss $ spiral rs -- �� ���� ��� ���
									1         -> read_loop resp sss $ spiral rs -- � ����� ���� ���
									otherwise -> read_loop resp sss ss          -- � ������� ���������� ��� �� ������

							where
								sm = sum $ map (\(RMultiSize r) -> r) (map snd rs)

								resp = ["*", showInt sm, "\r\n"]

								-- �������, �� ������ � ������� � ��� �� ����� (������). �� ������ ���������.
								-- print $ take 5 $ spiral [ ("a", 3), ("b", 4), ("c", 2), ("d", 0) ]
								spiral a = go a []
									where
										go [] []  = []
										go [] new = go new []
										go ((k,RMultiSize v):t) new
											| v == 0    =     go t new
											| otherwise = k : go t ((k, RMultiSize(v-1)):new)

								read_loop resp sss ss = go sss [] ss resp (sum $ map BS.length resp)
									where
										go sss                           []       []   resp resp_l = c_send resp >> return sss
										go []                            new_sss (h:t) resp resp_l = go new_sss [] t resp resp_l
										go ((s_addr, s_sock, s_recv, pr):old_sss) new_sss (h:t) resp resp_l
											| s_addr == h = parseWithNext server_parser_multi pr s_recv >>= rf
											| otherwise = go old_sss ((s_addr, s_sock, s_recv, pr):new_sss) (h:t) resp resp_l
													where
													rf (Left  ("", e)) = warn ["Parsing error server response (", cmd, ")"] >> fquit >> go old_sss ((s_addr, s_sock, s_recv, pr):new_sss) (h:t) resp resp_l
													rf (Left  (pr, e))  = warn ["Parsing error server response (", cmd, ")"] >> fquit >> go old_sss ((s_addr, s_sock, s_recv, pr):new_sss) (h:t) resp resp_l
													rf (Right (pr, r))  = case r of 
															RBulk r -> 
																case new_resp_l > 1024 of
																	True  -> c_send new_resp >>
																		go old_sss ((s_addr, s_sock, s_recv, pr):new_sss) (h:t) [] 0
																	False -> 
																		go old_sss ((s_addr, s_sock, s_recv, pr):new_sss) (h:t) new_resp new_resp_l
																where
																	arg        = arg2stream r
																	new_resp   = resp L.++ arg
																	new_resp_l = resp_l + (L.sum $ map BS.length arg)
