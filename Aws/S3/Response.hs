{-# LANGUAGE FlexibleInstances, OverloadedStrings, ScopedTypeVariables #-}
module Aws.S3.Response
where

import           Aws.Metadata
import           Aws.Response
import           Aws.S3.Error
import           Aws.S3.Metadata
import           Aws.Util
import           Control.Applicative
import           Control.Monad.Compose.Class
import           Data.Char
import           Data.Enumerator             ((=$))
import           Data.Maybe
import           Data.Word
import           Text.XML.Monad
import qualified Data.ByteString             as B
import qualified Data.ByteString.Char8       as B8
import qualified Data.Enumerator             as En
import qualified Data.Map                    as M
import qualified Data.Text                   as T
import qualified Network.HTTP.Enumerator     as HTTPE
import qualified Network.HTTP.Types          as HTTP
import qualified Text.XML.Enumerator.Parse   as XML

data S3Response a
    = S3Response {
        fromS3Response :: a
      , s3AmzId2 :: String
      , s3RequestId :: String
      }
    deriving (Show)

instance (S3ResponseIteratee a) => ResponseIteratee (S3Response a) where
    responseIteratee status headers = do
      let headerString = fromMaybe "" . fmap B8.unpack . flip lookup headers
      let amzId2 = headerString "x-amz-id-2"
      let requestId = headerString "x-amz-request-id"
      
      specific <- if status >= HTTP.status400
                  then fmap Left $ s3ErrorResponseIteratee status headers
                  else tryError $ s3ResponseIteratee status headers

      case specific of
        Left (err :: S3Error) -> En.throwError (setMetadata m err)
            where m = S3Metadata { s3MAmzId2 = amzId2, s3MRequestId = requestId }
        Right resp -> return S3Response {
                                        fromS3Response = resp
                                      , s3AmzId2 = amzId2
                                      , s3RequestId = requestId
                                      }

s3ErrorResponseIteratee :: HTTP.Status -> HTTP.ResponseHeaders -> En.Iteratee B.ByteString IO S3Error
s3ErrorResponseIteratee status headers = XML.parseBytes XML.decodeEntities =$ error
    where
      error = XML.force "Missing / invalid Error tag" $ XML.tagNoAttr "Error" errorContents
      errorContents = combine <$> XML.force "Invalid error tag contents" (XML.tagsPermuteRepetition id m ignoreTag)
      ignoreTag = XML.tagPredicate (const True) XML.ignoreAttrs (\_ -> XML.ignoreSiblings >> return Nothing)
      combine m = S3Error { 
                    s3StatusCode = status
                  , s3ErrorMetadata = Nothing
                  , s3ErrorCode = fromJust $ lookup "Code" m
                  , s3ErrorMessage = fromJust $ lookup "Message" m 
                  , s3ErrorResource = lookup "Resource" m
                  , s3ErrorHostId = lookup "HostId" m
                  , s3ErrorAccessKeyId = lookup "AWSAccessKeyId" m
                  , s3ErrorStringToSign = B.pack <$> (sequence . map readHex2 . words =<< lookup "StringToSignBytes" m)
                  }
      stringTag r = (r, XML.ignoreAttrs, \_ -> Just . T.unpack <$> XML.content)
      m = M.fromList $ map (\(c, r) -> (c, stringTag r)) 
          [
            ("Code", XML.repeatOnce)
          , ("Message", XML.repeatOnce)
          , ("Resource", XML.repeatOptional)
          , ("HostId", XML.repeatOptional)
          , ("AWSAccessKeyId", XML.repeatOptional)
          , ("StringToSignBytes", XML.repeatOptional)
          ]
      readHex2 [c1,c2] = do
        n1 <- readHex1 c1
        n2 <- readHex1 c2
        return . fromIntegral $ n1 * 16 + n2
      readHex2 _ = Nothing      
      readHex1 c | c >= '0' && c <= '9' = Just $ ord c - ord '0'
                 | c >= 'A' && c <= 'F' = Just $ ord c - ord 'A' + 10
                 | c >= 'a' && c <= 'f' = Just $ ord c - ord 'a' + 10
      readHex1 _                        = Nothing

                                                          

class S3ResponseIteratee a where
    s3ResponseIteratee :: HTTP.Status -> HTTP.ResponseHeaders -> En.Iteratee B.ByteString IO a

instance S3ResponseIteratee HTTPE.Response where
    s3ResponseIteratee = HTTPE.lbsIter
