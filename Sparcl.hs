{-# LANGUAGE OverloadedStrings #-}

module Sparcl (
    takeStr, takeBefore, takeInt, takeInteger, takeN,
    choice,
    bind, apL, apR, ap,
    Result (..), Parser (..), parse
) where

import Control.Applicative (Applicative(..))

import           Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as BS

data Result r = Done r ByteString | Partial | Fail deriving (Read, Show, Eq)
newtype Parser r = Parser { runParser :: ByteString -> Result r }

-- Генераторы парсеров

takeStr :: ByteString -> Parser ByteString
takeStr s =
    let s_l = BS.length s
    in
    Parser $ \ ss ->
        let ss_l = BS.length ss
            sss = BS.take s_l ss
        in
        if ss_l >= s_l
        then
            if sss == s
            then Done s (BS.drop s_l ss)
            else Fail
        else Partial


takeBefore :: ByteString -> Parser ByteString
takeBefore s = Parser $ \ ss -> case BS.breakSubstring s ss of
    (r, "")  -> if s > ss then Partial else Fail
    (r, t)   -> Done r t


takeInt :: Parser Int
takeInt = Parser $ \ ss -> case BS.readInt ss of
    Just (i, "") -> Partial
    Just (i, ss) -> Done i ss
    Nothing      -> if ss == "" then Partial else Fail

takeInteger :: Parser Integer
takeInteger = Parser $ \ ss -> case BS.readInteger ss of
    Just (i, "") -> Partial
    Just (i, ss) -> Done i ss
    Nothing      -> if ss == "" then Partial else Fail

takeN :: Int -> Parser ByteString
takeN s_l = Parser $ \ ss ->
    if s_l == 0 then Done "" ss else
    let (sss, tail) = BS.splitAt s_l ss in
    if sss == BS.empty
    then Partial
    else Done sss tail


-- Комбинаторы парсеров.

bind :: Parser t1 -> (t1 -> Parser t2) -> Parser t2
bind p pg = Parser $ \ ss ->
    case runParser p ss of
        Done r t -> runParser (pg r) t
        Partial  -> Partial
        Fail     -> Fail


apR :: Parser t1 -> Parser t2 -> Parser t2
apR p1 p2 = Parser $ \ ss ->
    case runParser p1 ss of
    Done r1 t1 -> runParser p2 t1
    Partial    -> Partial
    Fail       -> Fail


apL :: Parser t1 -> Parser t2 -> Parser t1
apL p1 p2 = Parser $ \ ss ->
    case runParser p1 ss of
        Done r1 t1 -> case runParser p2 t1 of
                        Done r2 t2 -> Done r1 t2
                        Partial    -> Partial
                        Fail       -> Fail
        Partial  -> Partial
        Fail     -> Fail


ap :: Parser (r1 -> r2) -> Parser r1 -> Parser r2
ap p1 p2 = Parser (\ ss ->
    case runParser p1 ss of
        Done r1 t1 -> case runParser p2 t1 of
                        Done r2 t2 -> Done (r1 r2) t2
                        Partial    -> Partial
                        Fail       -> Fail
        Partial  -> Partial
        Fail     -> Fail
    )


choice :: [Parser t] -> Parser t
choice ps = Parser $ \ ss ->
    let
        go (p:ps) is_Partial =
            case runParser p ss of
                Done r t -> Done r t
                Partial  -> go ps True
                Fail     -> go ps is_Partial
        go [] True  = Partial
        go [] False = Fail
    in go ps False


_pure :: a -> Parser a
_pure s = Parser (\ss -> Done s ss)


parse :: Parser a -> ByteString -> Result a
parse p s = runParser p s


instance Functor Parser where
    fmap f p = Parser $ \ ss ->
        case runParser p ss of
            Done r t -> Done (f r) t
            Partial  -> Partial
            Fail     -> Fail
    {-# INLINE fmap #-}


instance Monad Parser where
    return = _pure
    {-# INLINE return #-}
    m >>= k = bind m k
    {-# INLINE (>>=) #-}


instance Applicative Parser where
    pure = _pure
    {-# INLINE pure #-}
    (<*>) = ap
    -- Ниже - необязательно, выводиться автоматически.
    {-# INLINE (<*>) #-}
    (*>) = apR
    {-# INLINE (*>) #-}
    (<*) = apL
    {-# INLINE (<*) #-}
