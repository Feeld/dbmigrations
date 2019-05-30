{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE BangPatterns        #-}


module Database.Schema.Migrations.Tarball where
import Database.Schema.Migrations.Migration
    ( Migration(..)
    , emptyMigration
    )
import Database.Schema.Migrations.Store

import qualified Codec.Archive.Tar as Tar
import qualified Codec.Compression.GZip as Gzip
import Control.Exception (throwIO, Exception, catch, throw)
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.String.Conversions ( cs, (<>) )
import qualified Data.Text as T
import Data.Typeable ( Typeable )
import Data.Time.Clock ( UTCTime )
import Data.Time () -- for UTCTime Show instance

import Data.Yaml.YamlLight

import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString      as BS
import System.IO

type ContentMap = M.Map T.Text BS.ByteString

data TarballStoreError = NotImplemented
  deriving (Show, Exception)

--refactor out
data FilesystemStoreError = FilesystemStoreError String
                            deriving (Show, Typeable)

instance Exception FilesystemStoreError


tarballStore :: TarballStoreSettings -> IO MigrationStore
tarballStore settings = do
  eContentMap :: Either (Tar.FormatError, ContentMap) ContentMap  <- withFile (storePath settings) ReadMode $ \h -> do
    entries <- Tar.read . Gzip.decompress <$> LBS.hGetContents h
    pure $! Tar.foldlEntries step mempty entries
  case eContentMap of
    Right entryMap -> do
      pure MigrationStore
        { loadMigration = migrationFromEntry entryMap
        , saveMigration = \_ -> throwIO NotImplemented
        , getMigrations = pure $ catMaybes $ map (T.stripSuffix ".txt") $ M.keys entryMap
        , fullMigrationName = pure . T.unpack . T.dropEnd 4
        }
    Left (e, _) -> throwIO e
  where step :: ContentMap -> Tar.Entry -> ContentMap
        step !existingMap entry = case entryBs entry of
          Just bs -> M.insert (T.pack $ Tar.entryPath entry) bs existingMap
          Nothing -> existingMap

        entryBs :: Tar.Entry -> Maybe BS.ByteString
        entryBs entry = case Tar.entryContent entry of
          Tar.NormalFile bs _ -> Just $ LBS.toStrict bs
          _                   -> Nothing

newtype TarballStoreSettings = TBStore { storePath :: FilePath }


-- make everything compile
-- then change filepath to entry and call it migrationfromentry
-- to get filepath for name etc you need to say that path = EntryPath,
-- then parse the bytes from entry. Look at Tar documentations.
migrationFromEntry :: ContentMap -> T.Text -> IO (Either String Migration)
migrationFromEntry contentMap name = do
  (Right <$> process) `catch` (\(FilesystemStoreError s) -> return $ Left $ "Could not parse migration " ++ T.unpack name ++ ":" ++ s)
  where
    process = do
      case M.lookup name contentMap of
          Just bs -> do
            yaml <- parseYamlBytes bs
            -- Convert yaml structure into basic key/value map
            let fields = getFields yaml
                missing = missingFields fields

            case length missing of
              0 -> do
                let newM = emptyMigration name
                case migrationFromFields newM fields of
                  Nothing -> throwFS $ "Error in " ++ T.unpack name ++ ": unrecognized field found"
                  Just m -> return m
              _ -> throwFS $ "Error in " ++ T.unpack name ++ ": missing required field(s): " ++ (show missing)

          Nothing -> throwFS $ "Error in " ++ T.unpack name ++ ": not found in archive."


type FieldProcessor = T.Text -> Migration -> Maybe Migration

getFields :: YamlLight -> [(T.Text, T.Text)]
getFields (YMap mp) = map toPair $ M.assocs mp
    where
      toPair :: (YamlLight, YamlLight) -> (T.Text, T.Text)
      toPair (YStr k, YStr v) = (cs k, cs v)
      toPair (k, v) = throwFS $ "Error in YAML input; expected string key and string value, got " ++ (show (k, v))
getFields _ = throwFS "Error in YAML input; expected mapping"

missingFields :: [(T.Text, T.Text)] -> [T.Text]
missingFields fs =
    [ k | k <- requiredFields, not (k `elem` inputStrings) ]
    where
      inputStrings = map fst fs

requiredFields :: [T.Text]
requiredFields = [ "Apply"
                 , "Depends"
                 ]

filenameExtension :: String
filenameExtension = ".yml"

filenameExtensionTxt :: String
filenameExtensionTxt = ".txt"

addNewMigrationExtension :: FilePath -> FilePath
addNewMigrationExtension path = path <> filenameExtension

addMigrationExtension :: FilePath -> String -> FilePath
addMigrationExtension path ext = path <> ext

throwFS :: String -> a
throwFS = throw . FilesystemStoreError

migrationFromFields :: Migration -> [(T.Text, T.Text)] -> Maybe Migration
migrationFromFields m [] = Just m
migrationFromFields m ((name, value):rest) = do
  processor <- lookup name fieldProcessors
  newM <- processor value m
  migrationFromFields newM rest

fieldProcessors :: [(T.Text, FieldProcessor)]
fieldProcessors = [ ("Created", setTimestamp )
                  , ("Description", setDescription )
                  , ("Apply", setApply )
                  , ("Revert", setRevert )
                  , ("Depends", setDepends )
                  ]

setTimestamp :: FieldProcessor
setTimestamp value m = do
  ts <- case readTimestamp value of
          [(t, _)] -> return t
          _ -> fail "expected one valid parse"
  return $ m { mTimestamp = Just ts }

readTimestamp :: T.Text -> [(UTCTime, String)]
readTimestamp = reads . cs

setDescription :: FieldProcessor
setDescription desc m = Just $ m { mDesc = Just desc }

setApply :: FieldProcessor
setApply apply m = Just $ m { mApply = apply }

setRevert :: FieldProcessor
setRevert revert m = Just $ m { mRevert = Just revert }

setDepends :: FieldProcessor
setDepends depString m = Just $ m { mDeps = T.words depString }
