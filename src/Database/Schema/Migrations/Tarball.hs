{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE DeriveAnyClass      #-}


module Database.Schema.Migrations.Tarball where
import Database.Schema.Migrations.Migration
    ( Migration(..)
    , emptyMigration
    )
import Database.Schema.Migrations.Store

import qualified Codec.Archive.Tar as Tar
import qualified Codec.Compression.GZip as Gzip
import Control.Exception (throwIO, IOException, Exception, catch, throw)
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.String.Conversions ( cs, (<>) )
import qualified Data.Text as T
import Data.Typeable ( Typeable )
import Data.Time.Clock ( UTCTime )
import Data.Time () -- for UTCTime Show instance

import Data.Yaml.YamlLight

import qualified Data.ByteString.Lazy as LBS
import System.Directory ( getDirectoryContents, doesFileExist )
import System.FilePath ( (</>), takeExtension, dropExtension, takeBaseName )
import System.IO

type EntryMap = M.Map FilePath Tar.Entry

data TarballStoreError = NotImplemented
  deriving (Show, Exception)

--refactor out
data FilesystemStoreError = FilesystemStoreError String
                            deriving (Show, Typeable)

instance Exception FilesystemStoreError


tarballStore :: TarballStoreSettings -> IO MigrationStore
tarballStore settings = do
  eEntryMap :: Either (Tar.FormatError, EntryMap) EntryMap  <- withFile (storePath settings) ReadMode $ \h -> do
    entries <- Tar.read . Gzip.decompress <$> LBS.hGetContents h
    pure $ Tar.foldlEntries step mempty entries
  case eEntryMap of
    Right entryMap -> do
      pure MigrationStore
        { loadMigration = undefined
        , saveMigration = \_ -> throwIO NotImplemented
        , getMigrations = pure $ catMaybes $ map (T.stripSuffix ".txt" . T.pack) $ M.keys entryMap
        , fullMigrationName = pure . T.unpack . T.dropEnd 4
        }
    Left (e, _) -> throwIO e
  where step :: EntryMap -> Tar.Entry -> EntryMap
        step existingMap entry = M.insert (Tar.entryPath entry) entry existingMap

newtype TarballStoreSettings = TBStore { storePath :: FilePath }


-- make everything compile
-- then change filepath to entry and call it migrationfromentry
-- to get filepath for name etc you need to say that path = EntryPath,
-- then parse the bytes from entry. Look at Tar documentations.
migrationFromPath :: FilePath -> IO (Either String Migration)
migrationFromPath path = do
  let name = cs $ takeBaseName path
  (Right <$> process name) `catch` (\(FilesystemStoreError s) -> return $ Left $ "Could not parse migration " ++ path ++ ":" ++ s)

  where
    readMigrationFile = do
      ymlExists <- doesFileExist (addNewMigrationExtension path)
      if ymlExists
        then parseYamlFile (addNewMigrationExtension path) `catch` (\(e::IOException) -> throwFS $ show e)
        else parseYamlFile (addMigrationExtension path filenameExtensionTxt) `catch` (\(e::IOException) -> throwFS $ show e)

    process name = do
      yaml <- readMigrationFile

      -- Convert yaml structure into basic key/value map
      let fields = getFields yaml
          missing = missingFields fields

      case length missing of
        0 -> do
          let newM = emptyMigration name
          case migrationFromFields newM fields of
            Nothing -> throwFS $ "Error in " ++ (show path) ++ ": unrecognized field found"
            Just m -> return m
        _ -> throwFS $ "Error in " ++ (show path) ++ ": missing required field(s): " ++ (show missing)


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
