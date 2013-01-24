module MyForkManager where

import Control.Concurrent
import Control.Exception (mask, bracket, finally)

newtype ForkManager = FM ( MVar [ ( ThreadId, MVar () ) ] )


withForkManagerDo :: (ForkManager -> IO ()) -> IO ()
withForkManagerDo io =
	bracket
		(newMVar [] >>= return . FM)
		(waitForChildren)
		io
	where
		-- Ожидание завержения всех потомкив
		waitForChildren :: ForkManager -> IO ()
		waitForChildren (FM fm) = mapM_ (takeMVar . snd) =<< readMVar fm


forkWith :: ForkManager -> IO () -> IO ThreadId
forkWith (FM fm) io = mask $ \restore -> do
 	mvar   <- newEmptyMVar
 	thr_id <- forkIO $ finally (restore io) $ putMVar mvar ()
 	childs <- takeMVar fm
 	putMVar fm $ (thr_id,mvar):childs
 	return thr_id


killAllThread :: ForkManager -> IO ()
killAllThread (FM fm) = do
	my_thr_id <- myThreadId
	mapM_ killThread . filter (/= my_thr_id) . map fst =<< readMVar fm
	killThread my_thr_id
