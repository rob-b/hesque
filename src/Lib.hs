{-# LANGUAGE OverloadedStrings #-}

module Lib where
import Database.Redis
import Data.Either
import Data.Maybe
import Control.Monad.IO.Class
import qualified Data.ByteString.Char8 as B

mkProcessedKey :: B.ByteString -> B.ByteString
mkProcessedKey name = "resque:stat:processed:" `B.append` name

mkQueueKey :: B.ByteString -> B.ByteString
mkQueueKey name = "resque:queue:" `B.append` name

mkFailedKey :: B.ByteString -> B.ByteString
mkFailedKey name = "resque:stat:failed:" `B.append` name

mkStartedKey :: B.ByteString -> B.ByteString
mkStartedKey name = "resque:worker:" `B.append` name `B.append` ":started"

queuedJobs :: Connection -> B.ByteString -> IO ()
queuedJobs conn queueName =
  runRedis conn $ do
    queued <- llen $ mkQueueKey queueName
    liftIO $ B.putStr $ queueName `B.append` " "
    liftIO $ mapM_ print (rights [queued])

printGet :: Connection -> B.ByteString -> B.ByteString -> IO ()
printGet conn worker key =
  runRedis conn $ do
    value <- get key
    case value of
        Left reply -> do
          _ <- case reply of
              Error msg -> error (B.unpack msg)
              SingleLine msg -> error (B.unpack msg)
              _ -> error (show reply)
          error (show reply)
        Right result -> do
          let msg = fromMaybe "0" result
          liftIO $ B.putStr $ worker `B.append` " "
          liftIO $ B.putStrLn msg

main :: IO ()
main = do
  conn <- connect defaultConnectInfo
  runRedis conn $ do
    queues <- smembers "resque:queues"
    let queueNames = concat $ rights [queues]
    workers <- smembers "resque:workers"
    let workerNames = concat $ rights [workers]

    liftIO $ B.putStrLn "Queues:"
    liftIO $ mapM_ B.putStrLn queueNames

    liftIO $ B.putStrLn "Queued jobs:"
    liftIO $ mapM_ (queuedJobs conn) queueNames

    liftIO $ B.putStrLn "Workers:"
    liftIO $ mapM_ B.putStrLn workerNames
    liftIO $ B.putStrLn "Jobs processed:"
    liftIO $ mapM_ (\n -> printGet conn n (mkProcessedKey n)) workerNames

    liftIO $ B.putStrLn "Jobs failed:"
    failures <- llen "resque:failed"
    liftIO $ B.putStrLn $ "Total " `B.append ` either (const "0") (B.pack . show) failures
    liftIO $ mapM_ (\n -> printGet conn n (mkFailedKey n)) workerNames

    liftIO $ B.putStrLn "Worker start time:"
    liftIO $ mapM_ (\n -> printGet conn n (mkStartedKey n)) workerNames
