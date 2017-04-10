{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Prelude hiding (catch, getContents, concat)
import Control.Concurrent
import Control.Monad (mapM_, forM, forM_)
import Control.Exception (catch, throw, SomeException, IOException, AsyncException (ThreadKilled))
import Data.ByteString.Char8 (ByteString, pack, unpack, split, concat)
import Data.Maybe (maybe, fromJust)
import Data.Time.Clock
import Data.Tuple (fst, snd)
import System.IO
import System.Posix.Signals
import System.Environment (getArgs, getProgName)
import System.Console.GetOpt
import System.Exit
import Network.Socket hiding (recv)

import MyForkManager

import Network.Socket.ByteString (recv)

import RedisSharding


version = "1.1"


options :: [OptDescr (String, String)]
options = [
	Option [] ["host"]  (ReqArg (pair "host")  "IP")    "host",
	Option [] ["port"]  (ReqArg (pair "port")  "port")  "port",
	Option [] ["nodes"] (ReqArg (pair "nodes") "nodes") "nodes (host1:port1,host2:port2)",
	Option [] ["timeout"] (ReqArg (pair "timeout") "timeout") "timeout"
	]
	where
		pair :: a -> b -> (a, b)
		pair a b = (a, b)



main = withSocketsDo $ do
	installHandler sigPIPE Ignore Nothing

	hSetBuffering stdout LineBuffering

	printLog ["Start RedisSharding, (version - ", version, " strict)."]

	argv <- getArgs

	let get_opt = case getOpt Permute options argv of (opts, _, _) -> flip lookup opts
	 -- get_opt :: String -> Maybe String -- name -> value

	progName <- getProgName

	case get_opt "nodes" of
		Just _  -> return ()
		Nothing -> putStr (
				"Parameter 'nodes' is required.\n\nUsing example:\n" ++
				progName ++ " --nodes=10.1.1.2:6380,10.1.1.3:6380,...\n\n" ++
				"Others parameters:\n--host=10.1.1.1\n--port=6379\n" ++
				"--timeout=300 (0 - disable timeout)\n"
			) >> exitWith ExitSuccess

	host <- maybe (return iNADDR_ANY) inet_addr (get_opt "host")
	let port = (maybe 6379 (\a -> fromIntegral $ read a) (get_opt "port"))::PortNumber
	let servers = split ',' $ pack $ fromJust $ get_opt "nodes"
	let timeout = (maybe 300 (\a -> fromIntegral $ read a) (get_opt "timeout"))::Int

	sock <- socket AF_INET Stream defaultProtocol
	setSocketOption sock ReuseAddr 1
	bindSocket sock (SockAddrInet port host)
	listen sock 200

	let accepter = accept sock >>= \(c_sock, _) -> forkIO (welcome c_sock servers timeout) >> accepter

	accepter


welcome c_sock servers timeout = do
	setSocketOption c_sock KeepAlive 1

	addr2sMV <- newMVar [] -- Список пар "server address" => "server socket"

	catch (forM_ servers (server c_sock addr2sMV))
		(\e -> printLog [ pack (show (e::SomeException) ) ] >> clean_from_client c_sock addr2sMV)

	-- Получили список пар "server address" => "server socket" после заполнения, дальше он изментся не будет.
	addr2s <- readMVar addr2sMV
	let id_addr2s = addr2id addr2s

	let s_count = length servers

	withForkManagerDo $ \fm -> do
		let fquit = killAllThread fm

		waitMVar <- newEmptyMVar
		case timeout > 0 of
			True  -> forkWithQuit fm fquit (timer waitMVar timeout fquit) >> return ()
			False -> return ()

		cmds <- newChan -- Канал для команд
		let set_cmd c = writeChan cmds c
		let get_cmd   = getCurrentTime >>= putMVar waitMVar >> readChan cmds >>= \cmd -> takeMVar waitMVar >> return cmd

		toServersMVar <- newEmptyMVar
		toClientMVar  <- newEmptyMVar

		forkWithQuit fm fquit (servers_reader c_sock         id_addr2s get_cmd               toClientMVar fquit)
		forkWithQuit fm fquit (client_reader  c_sock s_count           set_cmd toServersMVar toClientMVar fquit)

		forkWithQuit fm fquit (servers_sender id_addr2s toServersMVar fquit)
		forkWithQuit fm fquit (client_sender  c_sock    toClientMVar  fquit)

		return ()

	clean_from_client c_sock addr2sMV

	where

		addr2id :: [(ByteString, Socket)] -> [(Int, Socket)]
		addr2id addr2s = fst $ foldr (\(_,s) (b,i) -> let i' = i + 1 in ((i',s):b,i')) ([],0) addr2s

		clean_from_client c_sock addr2sMV = do
			takeMVar addr2sMV >>= return . map snd >>= mapM_ sClose
			sClose c_sock

		-- Соединение с сервером
		server c_sock addr2sMV addr = do
			s_sock <- socket AF_INET Stream defaultProtocol
			ia     <- inet_addr (unpack host)
			connect s_sock (SockAddrInet port_number ia)
			setSocketOption s_sock KeepAlive 1

			modifyMVar_ addr2sMV (return . (++) [(addr,s_sock)])

			where
				[host, port] = split ':' addr
				port_number = fromIntegral (read (unpack port))::PortNumber


		forkWithQuit fm fquit io = forkWith fm (catch io (\e -> chokeIOException e >> fquit) )
			where
			chokeIOException :: IOException -> IO ()
			chokeIOException e = return ()

		timer waitMVar timeout fquit = do
			t0 <- readMVar waitMVar
			t  <- getCurrentTime
			let d = ceiling $ diffUTCTime t t0
			case d < timeout of
				True  -> threadDelay (1000000 * d) >> timer waitMVar timeout fquit
				False -> fquit
