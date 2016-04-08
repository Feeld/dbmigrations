{-# LANGUAGE ExistentialQuantification #-}
module Moo.Core
    ( AppT
    , CommandHandler
    , CommandOptions (..)
    , Command (..)
    , AppState (..)
    , Configuration (..)
    , DbConnDescriptor (..)
    , databaseTypes
    , envDatabaseName
    , envDatabaseType
    , envLinearMigrations
    , envStoreName
    , loadConfiguration) where

import Data.List.Split (wordsBy)
import Data.Char (isSpace)
import Control.Applicative
import Control.Monad.Reader (ReaderT)
import qualified Data.Configurator as C
import Data.Configurator.Types (Config, Configured)
import qualified Data.Text as T
import Data.Char (toLower)
import Database.HDBC.PostgreSQL (connectPostgreSQL)
import Database.HDBC.Sqlite3 (connectSqlite3)
import System.Environment (getEnvironment)
import Data.Maybe (fromMaybe)
import qualified Database.MySQL.Simple as MySQL
import qualified Database.MySQL.Base as MySQLB

import Database.Schema.Migrations ()
import Database.Schema.Migrations.Store (MigrationStore, StoreData)
import Database.Schema.Migrations.Backend
import Database.Schema.Migrations.Backend.HDBC
import Database.Schema.Migrations.Backend.MySQL

-- |The monad in which the application runs.
type AppT a = ReaderT AppState IO a

-- |The type of actions that are invoked to handle specific commands
type CommandHandler = StoreData -> AppT ()

-- |Application state which can be accessed by any command handler.
data AppState = AppState { _appOptions          :: CommandOptions
                         , _appCommand          :: Command
                         , _appRequiredArgs     :: [String]
                         , _appOptionalArgs     :: [String]
                         , _appStore            :: MigrationStore
                         , _appDatabaseConnStr  :: DbConnDescriptor
                         , _appDatabaseType     :: String
                         , _appStoreData        :: StoreData
                         , _appLinearMigrations :: Bool
                         , _appTimestampFilenames :: Bool
                         }

type ShellEnvironment = [(String, String)]

data Configuration = Configuration
    { _connectionString   :: String
    , _databaseType       :: String
    , _migrationStorePath :: FilePath
    , _linearMigrations   :: Bool
    , _timestampFilenames :: Bool
    } deriving Show

-- |Intermediate type used during config loading.
data LoadConfig = LoadConfig
    { _lcConnectionString   :: Maybe String
    , _lcDatabaseType       :: Maybe String
    , _lcMigrationStorePath :: Maybe FilePath
    , _lcLinearMigrations   :: Maybe Bool
    , _lcTimestampFilenames :: Maybe Bool
    } deriving Show

defConfigFile :: String
defConfigFile = "moo.cfg"

newLoadConfig :: LoadConfig
newLoadConfig = LoadConfig Nothing Nothing Nothing Nothing Nothing

validateLoadConfig :: LoadConfig -> Either String Configuration
validateLoadConfig (LoadConfig Nothing _ _ _ _) =
    Left "Invalid configuration: connection string not specified"
validateLoadConfig (LoadConfig _ Nothing _ _ _) =
    Left "Invalid configuration: database type not specified"
validateLoadConfig (LoadConfig _ _ Nothing _ _) =
    Left "Invalid configuration: migration store path not specified"
validateLoadConfig (LoadConfig (Just cs) (Just dt) (Just msp) lm ts) =
    Right $ Configuration cs dt msp (fromMaybe False lm) (fromMaybe False ts)

-- |Setters for fields of 'LoadConfig'.
lcConnectionString, lcDatabaseType, lcMigrationStorePath
    :: LoadConfig -> Maybe String -> LoadConfig
lcConnectionString c v   = c { _lcConnectionString   = v }
lcDatabaseType c v       = c { _lcDatabaseType       = v }
lcMigrationStorePath c v = c { _lcMigrationStorePath = v }

lcLinearMigrations :: LoadConfig -> Maybe Bool -> LoadConfig
lcLinearMigrations c v   = c { _lcLinearMigrations   = v }

lcTimestampFilenames :: LoadConfig -> Maybe Bool -> LoadConfig
lcTimestampFilenames c v = c { _lcTimestampFilenames = v }


-- | @f .= v@ invokes f only if v is 'Just'
(.=) :: (Monad m) => (a -> Maybe b -> a) -> m (Maybe b) -> m (a -> a)
(.=) f v' = do
    v <- v'
    return $ case v of
      Just _ -> flip f v
      _      -> id

-- |It's just @flip '<*>'@
(&) :: (Applicative m) => m a -> m (a -> b) -> m b
(&) = flip (<*>)

infixr 3 .=
infixl 2 &

applyEnvironment :: ShellEnvironment -> LoadConfig -> IO LoadConfig
applyEnvironment env lc =
    return lc & lcConnectionString   .= f envDatabaseName
              & lcDatabaseType       .= f envDatabaseType
              & lcMigrationStorePath .= f envStoreName
              & lcLinearMigrations   .= readFlag <$> f envLinearMigrations
              & lcTimestampFilenames .= readFlag <$> f envTimestampFilenames
    where f n = return $ lookup n env

applyConfigFile :: Config -> LoadConfig -> IO LoadConfig
applyConfigFile cfg lc =
    return lc & lcConnectionString   .= f envDatabaseName
              & lcDatabaseType       .= f envDatabaseType
              & lcMigrationStorePath .= f envStoreName
              & lcLinearMigrations   .= f envLinearMigrations
              & lcTimestampFilenames .= f envTimestampFilenames
    where
        f :: Configured a => String -> IO (Maybe a)
        f = C.lookup cfg . T.pack

-- |Loads config file (falling back to default one if not specified) and then
-- overrides configuration with an environment.
loadConfiguration :: Maybe FilePath -> IO (Either String Configuration)
loadConfiguration pth = do
    file <- maybe (C.load [C.Optional defConfigFile])
                  (\p -> C.load [C.Required p]) pth
    env <- getEnvironment
    cfg <- applyConfigFile file newLoadConfig >>= applyEnvironment env

    return $ validateLoadConfig cfg

-- |Converts @Just "on"@ and @Just "true"@ (case insensitive) to @True@,
-- anything else to @False@.
readFlag :: Maybe String -> Maybe Bool
readFlag Nothing  = Nothing
readFlag (Just v) = go $ map toLower v
    where
        go "on"    = Just True
        go "true"  = Just True
        go "off"   = Just False
        go "false" = Just False
        go _       = Nothing

-- |CommandOptions are those options that can be specified at the command
-- prompt to modify the behavior of a command.
data CommandOptions = CommandOptions { _configFilePath :: Maybe String
                                     , _test           :: Bool
                                     , _noAsk          :: Bool
                                     }

-- |A command has a name, a number of required arguments' labels, a
-- number of optional arguments' labels, and an action to invoke.
data Command = Command { _cName           :: String
                       , _cRequired       :: [String]
                       , _cOptional       :: [String]
                       , _cAllowedOptions :: [String]
                       , _cDescription    :: String
                       , _cHandler        :: CommandHandler
                       }

newtype DbConnDescriptor = DbConnDescriptor String

-- |The values of DBM_DATABASE_TYPE and their corresponding connection
-- factory functions.
databaseTypes :: [(String, String -> IO Backend)]
databaseTypes = [ ("postgresql", fmap hdbcBackend . connectPostgreSQL)
                , ("sqlite3", fmap hdbcBackend . connectSqlite3)
                , ("mysql", fmap mysqlBackend . connectMySQL)
                ]

-- A slightly hacky connection string parser for MySQL, because mysql-simple
-- doesn't come with one.
connectMySQL :: String -> IO MySQL.Connection
connectMySQL connectionString =
  let kvs =
        [(map toLower (trimlr k),trimlr v) | kvPair <-
                                              wordsBy (== ';') connectionString :: [String]
                                           , let (k,v) = case wordsBy (== '=') kvPair of
                                                           (k':v':_) -> (k',v')
                                                           [k'] -> (k',"")
                                                           [] -> error "impossible"]
      trimlr = takeWhile (not . isSpace) . dropWhile isSpace
      connInfo =
        MySQL.ConnectInfo
          <$> lookup "host" kvs
          <*> pure (read (fromMaybe "3306" (lookup "port" kvs)))
          <*> lookup "user" kvs
          <*> pure (fromMaybe "" (lookup "password" kvs))
          <*> lookup "database" kvs
          <*> pure [MySQLB.MultiStatements]
          <*> pure ""
          <*> pure Nothing
  in MySQL.connect (fromMaybe (error "Invalid connection string. Expected form: host=hostname; user=username; port=portNumber; database=dbname; password=pwd.")
                              connInfo)

envDatabaseType :: String
envDatabaseType = "DBM_DATABASE_TYPE"

envDatabaseName :: String
envDatabaseName = "DBM_DATABASE"

envStoreName :: String
envStoreName = "DBM_MIGRATION_STORE"

envLinearMigrations :: String
envLinearMigrations = "DBM_LINEAR_MIGRATIONS"

envTimestampFilenames :: String
envTimestampFilenames = "DBM_TIMESTAMP_FILENAMES"
