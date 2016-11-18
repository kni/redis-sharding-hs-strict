{-# LANGUAGE OverloadedStrings #-}

module RedisParser (
	multi_bulk_parser, server_parser, server_parser_multi, Reply(..),
	parseWithNext,
	cmd2stream, arg2stream
) where


import System.IO.Unsafe
import Prelude hiding (take)

import Data.Int (Int64)

import           Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as S

import Control.Applicative

import Sparcl
import qualified MyListBuf as LB

showInt :: Int64 -> ByteString
showInt a = S.pack $ show a


endOfLine = takeStr "\r\n"

get_bulk_size :: ByteString -> Parser Int64
get_bulk_size c = takeStr c *> fmap fromIntegral takeInteger <* endOfLine


get_bulk_value :: Int64 -> Parser (Maybe ByteString)
get_bulk_value (-1) = return Nothing
get_bulk_value n    = Just <$> takeN (fromIntegral n) <* endOfLine

get_bulk_arg :: Parser (Maybe ByteString)
get_bulk_arg = get_bulk_size "$" >>= get_bulk_value


multi_bulk_parser:: Parser (Maybe [Maybe ByteString])
multi_bulk_parser = get_bulk_size "*" >>=get_args []
	where
		get_args :: [Maybe ByteString] -> Int64 -> Parser (Maybe [Maybe ByteString])
		get_args as (-1) = return Nothing
		get_args as 0    = return $ Just $ reverse as
		get_args as n    = do
			a <- get_bulk_arg
		 	get_args (a:as) (n - 1)


data Reply = RInt Int64 | RInline ByteString | RBulk (Maybe ByteString) | RMultiSize Int64

server_parser :: Parser Reply
server_parser = choice [line, integer, bulk, multi_bulk_size]
	where
		line            = RInline    <$> do 
											h <- choice[takeStr "+", takeStr "-"]
											t <- takeBefore "\r\n"
											endOfLine
											return $ S.concat [h, t]
		integer         = RInt       <$> do takeStr ":" *> fmap fromIntegral takeInteger <* endOfLine
		bulk            = RBulk      <$> get_bulk_arg
		multi_bulk_size = RMultiSize <$> get_bulk_size "*"


server_parser_multi = RBulk <$> get_bulk_arg


trace :: ByteString -> a -> a
trace string expr = unsafePerformIO $ S.putStrLn string >> return expr


parseWithNext :: Monad m => Parser a -> ByteString -> m ByteString -> m (Either (ByteString, String) (ByteString, a))
parseWithNext p s next = go s
	where
	go s = case parse p s of
		Done r t  -> return $ Right (t, r)
		Fail      -> return $ Left (s, "Parsing Error")
		Partial   -> goP s

	goP s0 = do
		s <- next
		case s of
			"" -> return $ Left ("", "The End")
			s  -> go ( S.concat[s0, s] )



-- Преобразование команды (список аргументов) в строку, поток байтов, соответствующий протоколу redis.
cmd2stream :: [Maybe ByteString] -> LB.ListBuf
cmd2stream [] = LB.pack ["*0\r\n"]
cmd2stream as = LB.appendL h t
	where
	h = LB.pack ["*", (showInt $ fromIntegral $ length as), "\r\n"]
	t = map arg2stream as


-- Преобразование аргумента в строку, поток байтов, соответствующий протоколу redis.
arg2stream :: Maybe ByteString -> LB.ListBuf
arg2stream Nothing  = LB.pack ["$-1\r\n"]
arg2stream (Just s) = LB.pack ["$", (showInt $ fromIntegral $ S.length s), "\r\n", s, "\r\n"]
