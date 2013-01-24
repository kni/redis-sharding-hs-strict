{-# LANGUAGE OverloadedStrings, BangPatterns #-}

module MyListBuf (
	ListBuf, empty, null, length, append, appendL, appendB, appendBL, pack, toByteString
) where

import Prelude hiding (null, length)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B

import qualified Data.List as L


data ListBuf = LB {-# UNPACK #-} !Int ![ByteString]


empty :: ListBuf
empty = LB 0 []
{-# INLINE empty #-}


null :: ListBuf -> Bool
null (LB l _) = l <= 0
{-# INLINE null #-}


length :: ListBuf -> Int
length (LB l _) = l
{-# INLINE length #-}


append :: ListBuf -> ListBuf -> ListBuf
append (LB l1 ss1) (LB l2 ss2) = LB (l1 + l2) (foldr (\s2 s1 -> (s2:s1)) ss1 ss2)
{-# INLINE append #-}

appendL :: ListBuf -> [ListBuf] -> ListBuf
appendL lb lbs = foldr go lb $ reverse lbs
	where go s lb = append lb s
{-# INLINE appendL #-}


appendB :: ListBuf -> ByteString -> ListBuf
appendB (LB l ss) s = LB (l + B.length s) (s:ss)
{-# INLINE appendB #-}


appendBL :: ListBuf -> [ByteString] -> ListBuf
appendBL lb ss = foldr go lb $ reverse ss
	where go s lb = appendB lb s
{-# INLINE appendBL #-}


pack :: [ByteString] -> ListBuf
pack ss = appendBL empty ss
{-# INLINE pack #-}


toByteString :: ListBuf -> ByteString
toByteString (LB l ss) = B.concat $ L.reverse ss
{-# INLINE toByteString #-}
