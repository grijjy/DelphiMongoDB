unit Grijjy.MongoDB;
{ < Main interface to MongoDB }

{$INCLUDE 'Grijjy.inc'}

interface

uses
  winapi.Windows,
  System.SysUtils,
  System.Generics.Collections,
  Grijjy.Bson,
  Grijjy.Bson.IO,
  Grijjy.MongoDB.Protocol,
  Grijjy.MongoDB.Queries;

type
  { MongoDB validation types
    https://docs.mongodb.com/manual/reference/command/create/ }
  TgoMongoValidationLevel = (vlOff, vlStrict, vlModerate);

  TgoMongoValidationLevelHelper = record helper for TgoMongoValidationLevel
  public
    function ToString: string;
  end;

  TgoMongoValidationAction = (vaError, vaWarn);

  TgoMongoValidationActionHelper = record helper for TgoMongoValidationAction
  public
    function ToString: string;
  end;

  { MongoDB collation
    https://docs.mongodb.com/manual/reference/collation/ }
  TgoMongoCollationCaseFirst = (ccfUpper, ccfLower, ccfOff);

  TgoMongoCollationCaseFirstHelper = record helper for TgoMongoCollationCaseFirst
  public
    function ToString: string;
  end;

  TgoMongoCollationAlternate = (caNonIgnorable, caShifted);

  TgoMongoCollationAlternateHelper = record helper for TgoMongoCollationAlternate
  public
    function ToString: string;
  end;

  TgoMongoCollationMaxVariable = (cmvPunct, cmvSpace);

  TgoMongoCollationMaxVariableHelper = record helper for TgoMongoCollationMaxVariable
  public
    function ToString: string;
  end;

  TgoMongoCollation = record
  public
    Locale: string;
    CaseLevel: Boolean;
    CaseFirst: TgoMongoCollationCaseFirst;
    Strength: Integer;
    NumericOrdering: Boolean;
    Alternate: TgoMongoCollationAlternate;
    MaxVariable: TgoMongoCollationMaxVariable;
    Backwards: Boolean;
  end;

  { MongoDb dbStats
    https://docs.mongodb.com/manual/reference/command/dbStats/ }
  TgoMongoStatistics = record
  public
    Database: string;
    Collections: Integer;
    Views: Integer;
    Objects: Int64;
    AvgObjSize: Double;
    DataSize: Double;
    StorageSize: Double;
    NumExtents: Integer;
    Indexes: Integer;
    IndexSize: Double;
    ScaleFactor: Double;
    FsUsedSize: Double;
    FsTotalSize: Double;
  end;

  { MongoDb instances
    https://docs.mongodb.com/manual/reference/command/isMaster/ }
  TgoMongoInstance = record
  public
    Host: string;
    Port: Word;
  public
    constructor Create(AInstance: string); overload;
    constructor Create(AHost: string; APort: Word); overload;
  end;

  TgoMongoInstances = TArray<TgoMongoInstance>;

  TgoMongoInstanceInfo = record
  public
    Hosts: TgoMongoInstances;
    Arbiters: TgoMongoInstances;
    Primary: TgoMongoInstance;
    Me: TgoMongoInstance;
    SetName: string;
    SetVersion: Integer;
    IsMaster: Boolean;
    IsSecondary: Boolean;
    ArbiterOnly: Boolean;
    LocalTime: TDateTime;
    ConnectionId: Integer;
    ReadOnly: Boolean;
  end;

const
  { MongoDB collation default settings
    https://docs.mongodb.com/manual/reference/collation-locales-defaults/#collation-languages-locales }
  DEFAULTTGOMONGOCOLLATION: TgoMongoCollation = (Locale: 'en'; CaseLevel: false; CaseFirst: TgoMongoCollationCaseFirst.ccfOff; Strength: 1;
    NumericOrdering: false; Alternate: TgoMongoCollationAlternate.caNonIgnorable; MaxVariable: TgoMongoCollationMaxVariable.cmvSpace;
    Backwards: false;);

type
  { MongoDB error codes }
  TgoMongoErrorCode = (OK = 0, InternalError = 1, BadValue = 2, OBSOLETE_DuplicateKey = 3, NoSuchKey = 4, GraphContainsCycle = 5,
    HostUnreachable = 6, HostNotFound = 7, UnknownError = 8, FailedToParse = 9, CannotMutateObject = 10, UserNotFound = 11,
    UnsupportedFormat = 12, Unauthorized = 13, TypeMismatch = 14, Overflow = 15, InvalidLength = 16, ProtocolError = 17,
    AuthenticationFailed = 18, CannotReuseObject = 19, IllegalOperation = 20, EmptyArrayOperation = 21, InvalidBSON = 22,
    AlreadyInitialized = 23, LockTimeout = 24, RemoteValidationError = 25, NamespaceNotFound = 26, IndexNotFound = 27, PathNotViable = 28,
    NonExistentPath = 29, InvalidPath = 30, RoleNotFound = 31, RolesNotRelated = 32, PrivilegeNotFound = 33, CannotBackfillArray = 34,
    UserModificationFailed = 35, RemoteChangeDetected = 36, FileRenameFailed = 37, FileNotOpen = 38, FileStreamFailed = 39,
    ConflictingUpdateOperators = 40, FileAlreadyOpen = 41, LogWriteFailed = 42, CursorNotFound = 43, UserDataInconsistent = 45,
    LockBusy = 46, NoMatchingDocument = 47, NamespaceExists = 48, InvalidRoleModification = 49, ExceededTimeLimit = 50,
    ManualInterventionRequired = 51, DollarPrefixedFieldName = 52, InvalidIdField = 53, NotSingleValueField = 54, InvalidDBRef = 55,
    EmptyFieldName = 56, DottedFieldName = 57, RoleModificationFailed = 58, CommandNotFound = 59, OBSOLETE_DatabaseNotFound = 60,
    ShardKeyNotFound = 61, OplogOperationUnsupported = 62, StaleShardVersion = 63, WriteConcernFailed = 64, MultipleErrorsOccurred = 65,
    ImmutableField = 66, CannotCreateIndex = 67, IndexAlreadyExists = 68, AuthSchemaIncompatible = 69, ShardNotFound = 70,
    ReplicaSetNotFound = 71, InvalidOptions = 72, InvalidNamespace = 73, NodeNotFound = 74, WriteConcernLegacyOK = 75,
    NoReplicationEnabled = 76, OperationIncomplete = 77, CommandResultSchemaViolation = 78, UnknownReplWriteConcern = 79,
    RoleDataInconsistent = 80, NoMatchParseContext = 81, NoProgressMade = 82, RemoteResultsUnavailable = 83, DuplicateKeyValue = 84,
    IndexOptionsConflict = 85, IndexKeySpecsConflict = 86, CannotSplit = 87, SplitFailed_OBSOLETE = 88, NetworkTimeout = 89,
    CallbackCanceled = 90, ShutdownInProgress = 91, SecondaryAheadOfPrimary = 92, InvalidReplicaSetConfig = 93, NotYetInitialized = 94,
    NotSecondary = 95, OperationFailed = 96, NoProjectionFound = 97, DBPathInUse = 98, CannotSatisfyWriteConcern = 100,
    OutdatedClient = 101, IncompatibleAuditMetadata = 102, NewReplicaSetConfigurationIncompatible = 103, NodeNotElectable = 104,
    IncompatibleShardingMetadata = 105, DistributedClockSkewed = 106, LockFailed = 107, InconsistentReplicaSetNames = 108,
    ConfigurationInProgress = 109, CannotInitializeNodeWithData = 110, NotExactValueField = 111, WriteConflict = 112,
    InitialSyncFailure = 113, InitialSyncOplogSourceMissing = 114, CommandNotSupported = 115, DocTooLargeForCapped = 116,
    ConflictingOperationInProgress = 117, NamespaceNotSharded = 118, InvalidSyncSource = 119, OplogStartMissing = 120,
    DocumentValidationFailure = 121, OBSOLETE_ReadAfterOptimeTimeout = 122, NotAReplicaSet = 123, IncompatibleElectionProtocol = 124,
    CommandFailed = 125, RPCProtocolNegotiationFailed = 126, UnrecoverableRollbackError = 127, LockNotFound = 128,
    LockStateChangeFailed = 129, SymbolNotFound = 130, RLPInitializationFailed = 131, OBSOLETE_ConfigServersInconsistent = 132,
    FailedToSatisfyReadPreference = 133, ReadConcernMajorityNotAvailableYet = 134, StaleTerm = 135, CappedPositionLost = 136,
    IncompatibleShardingConfigVersion = 137, RemoteOplogStale = 138, JSInterpreterFailure = 139, InvalidSSLConfiguration = 140,
    SSLHandshakeFailed = 141, JSUncatchableError = 142, CursorInUse = 143, IncompatibleCatalogManager = 144, PooledConnectionsDropped = 145,
    ExceededMemoryLimit = 146, ZLibError = 147, ReadConcernMajorityNotEnabled = 148, NoConfigMaster = 149, StaleEpoch = 150,
    OperationCannotBeBatched = 151, OplogOutOfOrder = 152, ChunkTooBig = 153, InconsistentShardIdentity = 154,
    CannotApplyOplogWhilePrimary = 155, NeedsDocumentMove = 156, CanRepairToDowngrade = 157, MustUpgrade = 158, DurationOverflow = 159,
    MaxStalenessOutOfRange = 160, IncompatibleCollationVersion = 161, CollectionIsEmpty = 162, ZoneStillInUse = 163,
    InitialSyncActive = 164, ViewDepthLimitExceeded = 165, CommandNotSupportedOnView = 166, OptionNotSupportedOnView = 167,
    InvalidPipelineOperator = 168, CommandOnShardedViewNotSupportedOnMongod = 169, TooManyMatchingDocuments = 170,
    CannotIndexParallelArrays = 171, TransportSessionClosed = 172, TransportSessionNotFound = 173, TransportSessionUnknown = 174,
    QueryPlanKilled = 175, FileOpenFailed = 176, ZoneNotFound = 177, RangeOverlapConflict = 178, WindowsPdhError = 179,
    BadPerfCounterPath = 180, AmbiguousIndexKeyPattern = 181, InvalidViewDefinition = 182, ClientMetadataMissingField = 183,
    ClientMetadataAppNameTooLarge = 184, ClientMetadataDocumentTooLarge = 185, ClientMetadataCannotBeMutated = 186,
    LinearizableReadConcernError = 187, IncompatibleServerVersion = 188, PrimarySteppedDown = 189, MasterSlaveConnectionFailure = 190,
    OBSOLETE_BalancerLostDistributedLock = 191, FailPointEnabled = 192, NoShardingEnabled = 193, BalancerInterrupted = 194,
    ViewPipelineMaxSizeExceeded = 195, InvalidIndexSpecificationOption = 197, OBSOLETE_ReceivedOpReplyMessage = 198,
    ReplicaSetMonitorRemoved = 199, ChunkRangeCleanupPending = 200, CannotBuildIndexKeys = 201, NetworkInterfaceExceededTimeLimit = 202,
    ShardingStateNotInitialized = 203, TimeProofMismatch = 204, ClusterTimeFailsRateLimiter = 205, NoSuchSession = 206, InvalidUUID = 207,
    TooManyLocks = 208, StaleClusterTime = 209, CannotVerifyAndSignLogicalTime = 210, KeyNotFound = 211,
    IncompatibleRollbackAlgorithm = 212, DuplicateSession = 213, AuthenticationRestrictionUnmet = 214, DatabaseDropPending = 215,
    ElectionInProgress = 216, IncompleteTransactionHistory = 217, UpdateOperationFailed = 218, FTDCPathNotSet = 219,
    FTDCPathAlreadySet = 220, IndexModified = 221, CloseChangeStream = 222, IllegalOpMsgFlag = 223, JSONSchemaNotAllowed = 224,
    TransactionTooOld = 225,

    SocketException = 9001, OBSOLETE_RecvStaleConfig = 9996, NotMaster = 10107, CannotGrowDocumentInCappedNamespace = 10003,
    DuplicateKey = 11000, InterruptedAtShutdown = 11600, Interrupted = 11601, InterruptedDueToReplStateChange = 11602,
    OutOfDiskSpace = 14031, KeyTooLong = 17280, BackgroundOperationInProgressForDatabase = 12586,
    BackgroundOperationInProgressForNamespace = 12587, NotMasterOrSecondary = 13436, NotMasterNoSlaveOk = 13435, ShardKeyTooBig = 13334,
    StaleConfig = 13388, DatabaseDifferCase = 13297, OBSOLETE_PrepareConfigsFailed = 13104);

type
  { Is raised when there is an error writing to the database }
  EgoMongoDBWriteError = class(EgoMongoDBError)
{$REGION 'Internal Declarations'}
  private
    FErrorCode: TgoMongoErrorCode;
{$ENDREGION 'Internal Declarations'}
  public
    constructor Create(const AErrorCode: TgoMongoErrorCode; const AErrorMsg: string);
    { The MongoDB error code }
    property ErrorCode: TgoMongoErrorCode read FErrorCode;
  end;

type
  { Forward declarations }
  IgoMongoDatabase = interface;
  IgoMongoCollection = interface;
  igoMongoCursor = interface;
  tWriteCmd = Reference to procedure(Writer: IgoBsonWriter);

  { The client interface to MongoDB.
    This is the entry point for the MongoDB API.
    This interface is implemented in to TgoMongoClient class. }
  IgoMongoClient = interface
    ['{66FF5346-48F6-44E1-A46F-D8B958F06EA0}']
    { Returns an array with the names of all databases available to the client. }
    function ListDatabaseNames: TArray<string>;

    { Returns an array of documents describing all databases available to the
      client (one document per database). The structure of each document is
      described here:
      https://docs.mongodb.com/manual/reference/command/listDatabases/ }
    function ListDatabases: TArray<TgoBsonDocument>;

    { Returns a document that describes the role of the mongod instance. If the optional
      field saslSupportedMechs is specified, the command also returns an array of
      SASL mechanisms used to create the specified user’s credentials.
      If the instance is a member of a replica set, then isMaster returns a subset
      of the replica set configuration and status including whether or not the instance
      is the primary of the replica set.

      described here:
      https://docs.mongodb.com/manual/reference/command/isMaster/
    }
    function GetInstanceInfo(const ASaslSupportedMechs: string = ''; const AComment: string = ''): TgoMongoInstanceInfo;
    function IsMaster: Boolean;

    { Issue an admin command that is supposed to return ONE document }
    function AdminCommand(CommandToIssue: tWriteCmd): igoMongoCursor;
    { Issue a logRotate command.
      https://www.mongodb.com/docs/manual/reference/command/logRotate/ }
    function LogRotate: Boolean;
    { Query build info of the current Mongod
      https://www.mongodb.com/docs/manual/reference/command/buildInfo/ }
    function BuildInfo: TgoBsonDocument;
    { Query system/platform info of the current Mongod server
      https://www.mongodb.com/docs/manual/reference/command/hostInfo/ }
    function HostInfo: TgoBsonDocument;
    { Query build-level feature settings
      https://www.mongodb.com/docs/manual/reference/command/features/ }
    function Features: TgoBsonDocument;
    { Query to find out MaxWireVersion }
    function Hello: TgoBsonDocument;
    { Drops the database with the specified name.
      Parameters:
      AName: The name of the database to drop. }
    procedure DropDatabase(const AName: string);
    { Gets a database.

      Parameters:
      AName: the name of the database.
      Returns:
      An implementation of the database.

      NOTE: If a database with the given name does not exist, then it will be
      automatically created as soon as you start writing to it.

      NOTE: This method is light weight and doesn't actually open the database
      yet. The database is only opened once you start reading, writing or
      querying it. }
    function GetDatabase(const AName: string): IgoMongoDatabase;
    function GetGlobalReadPreference: tgoMongoReadPreference;
    procedure SetGlobalReadPreference(const Value: tgoMongoReadPreference);

    { GlobalReadPreference sets the global ReadPreference for all objects (database, collection etc)
      that do not have an individual specific ReadPreference. }
    property GlobalReadPreference: tgoMongoReadPreference read GetGlobalReadPreference write SetGlobalReadPreference;
  end;

  { Represents a database in MongoDB.
    Instances of this interface are aquired by calling
    IgoMongoClient.GetDatabase. }
  IgoMongoDatabase = interface
    ['{5164D7B1-74F5-45F1-AE22-AB5FFC834590}']
{$REGION 'Internal Declarations'}
    function _GetClient: IgoMongoClient;
    function _GetName: string;
{$ENDREGION 'Internal Declarations'}
    { Returns an array with the names of all collections in the database. }
    function ListCollectionNames: TArray<string>;

    { Returns an array of documents describing all collections in the database
      (one document per collection). The structure of each document is
      described here:
      https://docs.mongodb.com/manual/reference/method/db.getCollectionInfos/ }
    function ListCollections: TArray<TgoBsonDocument>;

    { Drops the collection with the specified name.

      Parameters:
      AName: The name of the collection to drop. }
    procedure DropCollection(const AName: string);

    { Gets a collection.

      Parameters:
      AName: the name of the collection.

      Returns:
      An implementation of the collection.

      NOTE: If a collection with the given name does not exist in this database,
      then it will be automatically created as soon as you start writing to it.

      NOTE: This method is light weight and doesn't actually open the collection
      yet. The collection is only opened once you start reading, writing or
      querying it. }
    function GetCollection(const AName: string): IgoMongoCollection;

    { Creates a collection.

      All parameters are described here:
      https://docs.mongodb.com/manual/reference/command/create/ }
    function CreateCollection(const AName: string; const ACapped: Boolean; const AMaxSize: Int64; const AMaxDocuments: Int64;
      const AValidationLevel: TgoMongoValidationLevel; const AValidationAction: TgoMongoValidationAction; const AValidator: TgoBsonDocument;
      const ACollation: TgoMongoCollation): Boolean;

    { Rename a collection.

      All parameters are described here:
      https://docs.mongodb.com/manual/reference/command/renameCollection/ }
    function RenameCollection(const AFromNamespace, AToNamespace: string; const ADropTarget: Boolean = false): Boolean;

    { Get database statistics.

      All parameters are described here:
      https://docs.mongodb.com/manual/reference/command/dbStats/ }
    function GetDbStats(const AScale: Integer): TgoMongoStatistics;

    { Issue a command against the database that returns one document.
      Similar to AdminCommand. }
    function AdminCommand(CommandToIssue: tWriteCmd): igoMongoCursor;

    { Issue a command against the database that returns one document.
      Similar to AdminCommand. }
    function Command(CommandToIssue: tWriteCmd): igoMongoCursor;
    function GetReadPreference: tgoMongoReadPreference;
    procedure SetReadPreference(const Value: tgoMongoReadPreference);

    { The client used for this database. }
    property Client: IgoMongoClient read _GetClient;

    { The name of the database. }
    property name: string read _GetName;
    { setting ReadPreference on the database will override the global readpreference }
    property ReadPreference: tgoMongoReadPreference read GetReadPreference write SetReadPreference;
  end;

  { Represents a cursor to the documents returned from one of the
    IgoMongoCollection.Find methods. }
  igoMongoCursor = interface
    ['{18813F27-1B41-453C-86FE-E98AFEB3D905}']
    { Allows for..in enumeration over all documents in the cursor. }
    function GetEnumerator: TEnumerator<TgoBsonDocument>;

    { Converts all documents in the cursor to an array.
      Note that this can be time consuming and result in a large array,
      depending on the number of documents in the cursor.

      Returns:
      An array of documents in the cursor. }
    function ToArray: TArray<TgoBsonDocument>;
  end;

  { Represents a collection in a MongoDB database.
    Instances of this interface are aquired by calling
    IgoMongoDatabase.GetCollection. }
  IgoMongoCollection = interface
    ['{9822579B-1682-4FAC-81CF-A4B239777812}']
{$REGION 'Internal Declarations'}
    function _GetDatabase: IgoMongoDatabase;
    function _GetName: string;
{$ENDREGION 'Internal Declarations'}
    { Inserts a single document.

      Parameters:
      ADocument: The document to insert.

      Returns:
      True if document has been successfully inserted. False if not. }
    function InsertOne(const ADocument: TgoBsonDocument): Boolean;

    { Inserts many documents.

      Parameters:
      ADocuments: The documents to insert.
      AOrdered: Optional. If True, perform an ordered insert of the documents
      in the array, and if an error occurs with one of documents, MongoDB
      will return without processing the remaining documents in the array.
      If False, perform an unordered insert, and if an error occurs with one
      of documents, continue processing the remaining documents in the
      array.
      Defaults to true.

      Returns:
      The number of inserted documents. }
    function InsertMany(const ADocuments: array of TgoBsonDocument; const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TArray<TgoBsonDocument>; const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TEnumerable<TgoBsonDocument>; const AOrdered: Boolean = True): Integer; overload;

    { Deletes a single document.

      Parameters:
      AFilter: filter containing query operators to search for the document
      to delete.

      Returns:
      True if a document matching the filter has been found and it has
      been successfully deleted. }
    function DeleteOne(const AFilter: TgoMongoFilter): Boolean;

    { Deletes all documents that match a filter.

      Parameters:
      AFilter: filter containing query operators to search for the documents
      to delete.
      AOrdered: Optional. If True, then when a delete statement fails, return
      without performing the remaining delete statements. If False, then
      when a delete statement fails, continue with the remaining delete
      statements, if any.
      Defaults to true.

      Returns:
      The number of documents deleted. }
    function DeleteMany(const AFilter: TgoMongoFilter; const AOrdered: Boolean = True): Integer;

    { Updates a single document.

      Parameters:
      AFilter: filter containing query operators to search for the document
      to update.
      AUpdate: the update definition that specifies how the document should
      be updated.
      AUpsert: (optional) upsert flag. If True, perform an insert if no
      documents match the query. Defaults to False.

      Returns:
      True if a document matching the filter has been found and it has
      been successfully updated. }
    function UpdateOne(const AFilter: TgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert: Boolean = false): Boolean;

    { Updates all documents that match a filter.

      Parameters:
      AFilter: filter containing query operators to search for the documents
      to update.
      AUpdate: the update definition that specifies how the documents should
      be updated.
      AUpsert: (optional) upsert flag. If True, perform an insert if no
      documents match the query. Defaults to False.
      AOrdered: Optional. If True, then when an update statement fails, return
      without performing the remaining update statements. If False, then
      when an update statement fails, continue with the remaining update
      statements, if any.
      Defaults to true.

      Returns:
      The number of documents that match the filter. The number of documents
      that is actually updated may be less than this in case an update did
      not result in the change of one or more documents. }
    function UpdateMany(const AFilter: TgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert: Boolean = false;
      const AOrdered: Boolean = True): Integer;

    { Finds the documents matching the filter.

      Parameters:
      AFilter: (optional) filter containing query operators to search for
      documents that match the filter. If not specified, then all documents
      in the collection are returned.
      AProjection: (optional) projection that specifies the fields to return
      in the documents that match the query filter. If not specified, then
      all fields are returned.
      ASort: (optional) sort modifier, used to sort the results. Note: an
      exception is raised when the result set is very large (32MB or larger)
      and cannot be sorted.

      Returns:
      An enumerable of documents that match the filter. The enumerable will
      be empty if there are no documents that match the filter.
      Enumerating over the result may trigger additional calls to the MongoDB
      server. }
    function Find(const AFilter: TgoMongoFilter; const AProjection: TgoMongoProjection): igoMongoCursor; overload;
    function Find(const AFilter: TgoMongoFilter): igoMongoCursor; overload;
    function Find(const AProjection: TgoMongoProjection): igoMongoCursor; overload;
    function Find: igoMongoCursor; overload;
    function Find(const AFilter: TgoMongoFilter; const ASort: TgoMongoSort): igoMongoCursor; overload;
    function Find(const AFilter: TgoMongoFilter; const AProjection: TgoMongoProjection; const ASort: TgoMongoSort;
      const ANumberToSkip: Integer = 0): igoMongoCursor; overload;

    { Finds the first document matching the filter.

      Parameters:
      AFilter: filter containing query operators to search for the document
      that matches the filter.
      ASort: (optional) use this to find the maximum or minimum value of a field.
      An empty filter (tgomongofilter.Empty) with ASort=tgomongosort.Descending('price')
      will return the document having the highest 'price'.
      For best performance, use indexes in the collection.
      AProjection: (optional) projection that specifies the fields to return
      in the document that matches the query filter. If not specified, then
      all fields are returned.

      Returns:
      The first document that matches the filter. If no documents match the
      filter, then a null-documents is returned (call its IsNil method to
      check for this). }
    function FindOne(const AFilter: TgoMongoFilter; const AProjection: TgoMongoProjection): TgoBsonDocument; overload;
    function FindOne(const AFilter: TgoMongoFilter): TgoBsonDocument; overload;
    function FindOne(const AFilter: TgoMongoFilter; const ASort: TgoMongoSort): TgoBsonDocument; overload;
    function FindOne(const AFilter: TgoMongoFilter; const AProjection: TgoMongoProjection; const ASort: TgoMongoSort)
      : TgoBsonDocument; overload;

    { Counts the number of documents matching the filter.

      Parameters:
      AFilter: (optional) filter containing query operators to search for
      documents that match the filter. If not specified, then the total
      number of documents in the collection is returned.

      Returns:
      The number of documents that match the filter. }
    function Count: Integer; overload;
    function Count(const AFilter: TgoMongoFilter): Integer; overload;

    { Creates an index in the current collection.

      Parameters:
      AName: Name of the index.
      AKeyFields: List of fields to build the index.
      AUnique: Defines a unique index.

      Returns:
      Created or not. }
    function CreateIndex(const AName: string; const AKeyFields: array of string; const AUnique: Boolean = false): Boolean;

    { Creates an text index in the current collection.

      Parameters:
      AName: Name of the index.
      AFields: List of fields to build the index.
      ALanguageOverwriteField: Defines a field that contains the language to
      use for a specific document.
      ADefaultLanguage: Defines the default language for internal indexing.

      See https://docs.mongodb.com/manual/reference/text-search-languages/#text-search-languages
      for language definitions.

      Returns:
      Created or not. }
    function CreateTextIndex(const AName: string; const AFields: array of string; const ALanguageOverwriteField: string = '';
      const ADefaultLanguage: string = 'en'): Boolean;

    { Drops an index in the current collection.

      Parameters:
      AName: Name of the index.

      Returns:
      Dropped or not. }
    function DropIndex(const AName: string): Boolean; overload;

    { List all index names in the current collection.

      Returns:
      TArray<String> of index names. }
    function ListIndexNames: TArray<string>; overload;
    function ListIndexes: TArray<TgoBsonDocument>; overload;

    { Return statistics about the collection, see
      https://www.mongodb.com/docs/manual/reference/command/collStats }
    function Stats: TgoBsonDocument;
    function GetReadPreference: tgoMongoReadPreference;
    procedure SetReadPreference(const Value: tgoMongoReadPreference);

    { The database that contains this collection. }
    property Database: IgoMongoDatabase read _GetDatabase;

    { The name of the collection. }
    property name: string read _GetName;

    { setting ReadPreference on the collection will override the global readpreference }
    property ReadPreference: tgoMongoReadPreference read GetReadPreference write SetReadPreference;
  end;

type
  { Can be passed to the constructor of TgoMongoClient to customize the
    client settings. }
  TgoMongoClientSettings = record
  public
    { Timeout waiting for connection, in milliseconds.
      Defaults to 5000 (5 seconds) }
    ConnectionTimeout: Integer;

    { Timeout waiting for partial or complete reply events, in milliseconds.
      Defaults to 5000 (5 seconds) }
    ReplyTimeout: Integer;

    { Default query flags }
    QueryFlags: TgoMongoQueryFlags;

    { Tls enabled }
    Secure: Boolean;

    { X.509 Certificate in PEM format, if any }
    Certificate: TBytes;

    { X.509 Private key in PEM format, if any }
    PrivateKey: TBytes;

    { Password for private key, optional }
    PrivateKeyPassword: string;

    { Authentication mechanism }
    AuthMechanism: TgoMongoAuthMechanism;

    { Authentication database }
    AuthDatabase: string;

    { Authentication username }
    Username: string;

    { Authentication password }
    Password: string;

    GlobalReadPreference: tgoMongoReadPreference;
  public
    { Creates a settings record with the default settings }
    class function Create: TgoMongoClientSettings; static;
  end;

type
  { Implements IgoMongoClient.
    This is the main entry point to the MongoDB API. }
  TgoMongoClient = class(TInterfacedObject, IgoMongoClient)
  public const
    { Default host address of the MongoDB server. }
    DEFAULT_HOST = 'localhost';
    { Default connection port. }
    DEFAULT_PORT = 27017;
{$REGION 'Internal Declarations'}
  private
    FProtocol: TgoMongoProtocol;
    function GetGlobalReadPreference: tgoMongoReadPreference;
    procedure SetGlobalReadPreference(const Value: tgoMongoReadPreference);
  protected
    { IgoMongoClient }
    function ListDatabaseNames: TArray<string>;
    function ListDatabases: TArray<TgoBsonDocument>;
    procedure DropDatabase(const AName: string);
    function GetDatabase(const AName: string): IgoMongoDatabase;
    function GetInstanceInfo(const ASaslSupportedMechs: string = ''; const AComment: string = ''): TgoMongoInstanceInfo;
    function IsMaster: Boolean;
    function AdminCommand(CommandToIssue: tWriteCmd): igoMongoCursor;
    function LogRotate: Boolean;
    function BuildInfo: TgoBsonDocument;
    function HostInfo: TgoBsonDocument;
    function Features: TgoBsonDocument;
    function Hello: TgoBsonDocument;
  protected
    property Protocol: TgoMongoProtocol read FProtocol;
{$ENDREGION 'Internal Declarations'}
  public
    { Creates a client interface to MongoDB.

      Parameters:
      AHost: (optional) host address of the MongoDB server to connect to.
      Defaults to 'localhost'.
      APort: (optional) connection port. Defaults to 27017.
      ASettings: (optional) client settings.

      NOTE: The constructor is light weight and does NOT connect to the server
      until the first read, write or query operation. }
    constructor Create(const AHost: string = DEFAULT_HOST; const APort: Integer = DEFAULT_PORT); overload;
    constructor Create(const AHost: string; const APort: Integer; const ASettings: TgoMongoClientSettings); overload;
    constructor Create(const ASettings: TgoMongoClientSettings); overload;
    destructor Destroy; override;

    { GlobalReadPreference sets the global ReadPreference for all objects (database, collection etc)
      that do not have an individual specific ReadPreference. }
    property GlobalReadPreference: tgoMongoReadPreference read GetGlobalReadPreference write SetGlobalReadPreference;
  end;

resourcestring
  RS_MONGODB_CONNECTION_ERROR = 'Error connecting to the MongoDB database';
  RS_MONGODB_GENERIC_ERROR = 'Unspecified error while performing MongoDB operation';

implementation

uses
  System.Math;

{$POINTERMATH ON}

{ If no reply was received within timeout seconds, throw an exception }
procedure HandleTimeout(const AReply: IgoMongoReply); inline;
begin
  if (AReply = nil) then
    raise EgoMongoDBConnectionError.Create(RS_MONGODB_CONNECTION_ERROR);
end;

{ If timeout, or error message, throw exception }
function HandleCommandReply(const AReply: IgoMongoReply; const AErrorToIgnore: TgoMongoErrorCode = TgoMongoErrorCode.OK): Integer;
var
  Doc, ErrorDoc: TgoBsonDocument;
  Value: TgoBsonValue;
  Values: TgoBsonArray;
  OK: Boolean;
  ErrorCode: TgoMongoErrorCode;
  ErrorMsg: string;
begin
  HandleTimeout(AReply);

  Doc := AReply.FirstDoc;
  if Doc.IsNil then
    { Everything OK }
    Exit(0);

  { Return number of documents affected }
  Result := Doc['n'];

  OK := Doc['ok'];
  if (not OK) then
  begin
    { Check for top-level error }
    Word(ErrorCode) := Doc['code'];

    { Check for expected error }
    if (AErrorToIgnore <> TgoMongoErrorCode.OK) and (ErrorCode = AErrorToIgnore) then
      Exit;

    if (ErrorCode <> TgoMongoErrorCode.OK) then
    begin
      ErrorMsg := Doc['errmsg'];
      raise EgoMongoDBWriteError.Create(ErrorCode, ErrorMsg);
    end;

    { If there is no top-level error, then check for Write Error(s).
      Raise exception for first write error found. }
    if (Doc.TryGetValue('writeErrors', Value)) then
    begin
      Values := Value.AsBsonArray;
      if (Values.Count > 0) then
      begin
        ErrorDoc := Values.Items[0].AsBsonDocument;
        Word(ErrorCode) := ErrorDoc['code'];
        ErrorMsg := ErrorDoc['errmsg'];
        raise EgoMongoDBWriteError.Create(ErrorCode, ErrorMsg);
      end;
    end;

    { If there are no write errors either, then check for write concern error. }
    if (Doc.TryGetValue('writeConcernError', Value)) then
    begin
      ErrorDoc := Value.AsBsonDocument;
      Word(ErrorCode) := ErrorDoc['code'];
      ErrorMsg := ErrorDoc['errmsg'];
      raise EgoMongoDBWriteError.Create(ErrorCode, ErrorMsg);
    end;

    { Could not detect any errors in reply. Raise generic error. }
    raise EgoMongoDBError.Create(RS_MONGODB_GENERIC_ERROR);
  end;
end;

type
  { Implements IgoMongoDatabase }
  TgoMongoDatabase = class(TInterfacedObject, IgoMongoDatabase)
{$REGION 'Internal Declarations'}
  private
    FClient: IgoMongoClient;
    FProtocol: TgoMongoProtocol; // Reference
    FName: string;
    FReadPreference: tgoMongoReadPreference;
    function GetReadPreference: tgoMongoReadPreference;
    procedure SetReadPreference(const Value: tgoMongoReadPreference);
    procedure SpecifyDB(const AWriter: IgoBsonWriter);
    procedure SpecifyReadPreference(const AWriter: IgoBsonWriter);
  protected
    { IgoMongoDatabase }
    function _GetClient: IgoMongoClient;
    function _GetName: string;
    function ListCollectionNames: TArray<string>;
    function ListCollections: TArray<TgoBsonDocument>;
    procedure DropCollection(const AName: string);
    function GetCollection(const AName: string): IgoMongoCollection;
    function CreateCollection(const AName: string; const ACapped: Boolean; const AMaxSize: Int64; const AMaxDocuments: Int64;
      const AValidationLevel: TgoMongoValidationLevel; const AValidationAction: TgoMongoValidationAction; const AValidator: TgoBsonDocument;
      const ACollation: TgoMongoCollation): Boolean;
    function RenameCollection(const AFromNamespace, AToNamespace: string; const ADropTarget: Boolean = false): Boolean;
    function GetDbStats(const AScale: Integer): TgoMongoStatistics;
    function Command(CommandToIssue: tWriteCmd): igoMongoCursor;
    function AdminCommand(CommandToIssue: tWriteCmd): igoMongoCursor;

  protected
    property Protocol: TgoMongoProtocol read FProtocol;
    property name: string read FName;
{$ENDREGION 'Internal Declarations'}
  public
    constructor Create(const AClient: TgoMongoClient; const AName: string);
    property ReadPreference: tgoMongoReadPreference read GetReadPreference write SetReadPreference;
  end;

type
  { Implements IgoMongoCursor }
  TgoMongoCursor = class(TInterfacedObject, igoMongoCursor)
{$REGION 'Internal Declarations'}
  private type
    TEnumerator = class(TEnumerator<TgoBsonDocument>)
    private
      FProtocol: TgoMongoProtocol; // Reference
      FDatabaseName: string;
      FCollectionName: string;
      FPage: TArray<TBytes>;
      FCursorId: Int64;
      FIndex: Integer;
      FReadPreference: tgoMongoReadPreference;
    private
      procedure GetMore;
      procedure SpecifyDB(const Writer: IgoBsonWriter);
      procedure SpecifyReadPreference(const AWriter: IgoBsonWriter);
    protected
      function DoGetCurrent: TgoBsonDocument; override;
      function DoMoveNext: Boolean; override;
    public
      destructor Destroy; override;
      constructor Create(const AProtocol: TgoMongoProtocol; AReadPreference: tgoMongoReadPreference;
        const ADatabaseName, ACollectionName: string; const APage: TArray<TBytes>; const ACursorId: Int64);
    end;
  private
    FProtocol: TgoMongoProtocol; // Reference
    FDatabaseName: string;
    FCollectionName: string;
    FInitialPage: TArray<TBytes>;
    FInitialCursorId: Int64;
    FReadPreference: tgoMongoReadPreference;
  public
    { IgoMongoCursor }
    function GetEnumerator: TEnumerator<TgoBsonDocument>;
    function ToArray: TArray<TgoBsonDocument>;
  public
    constructor Create(const AProtocol: TgoMongoProtocol; AReadPreference: tgoMongoReadPreference;
      const ADatabaseName, ACollectionName: string; const AInitialPage: TArray<TBytes>; const AInitialCursorId: Int64); overload;

    constructor Create(const AProtocol: TgoMongoProtocol; AReadPreference: tgoMongoReadPreference; const aNameSpace: string;
      const AInitialPage: TArray<TBytes>; const AInitialCursorId: Int64); overload;

{$ENDREGION 'Internal Declarations'}
  end;

procedure DoSpecifyReadPreference(AReadPreference: tgoMongoReadPreference; const AWriter: IgoBsonWriter);
begin
  if AReadPreference <> tgoMongoReadPreference.Primary then
  begin
    AWriter.WriteStartDocument('$readPreference');
    case AReadPreference of
      tgoMongoReadPreference.Primary:
        AWriter.WriteString('mode', 'primary');
      tgoMongoReadPreference.primaryPreferred:
        AWriter.WriteString('mode', 'primaryPreferred');
      tgoMongoReadPreference.secondary:
        AWriter.WriteString('mode', 'secondary');
      tgoMongoReadPreference.secondaryPreferred:
        AWriter.WriteString('mode', 'secondaryPreferred');
      tgoMongoReadPreference.nearest:
        AWriter.WriteString('mode', 'nearest');
    end;
    AWriter.WriteEndDocument;
  end;
end;

function HasCursor(const ADoc: TgoBsonDocument; var Cursor: TgoBsonDocument; var CursorID: Int64; var Namespace: string): Boolean; inline;
var
  temp: TgoBsonValue;
begin
  Cursor.SetNil;
  CursorID := 0;
  Namespace := '';
  Result := (ADoc.TryGetValue('cursor', temp));
  if Result then
  begin
    Cursor := temp.AsBsonDocument;
    CursorID := Cursor['id']; // 0=cursor exhausted, else more data can be pulled
    Namespace := Cursor.Get('ns', '').ToString(); // databasename.CollectionNameOrCommand
  end;
end;

function firstBatchToCursor(const ADoc: TgoBsonDocument; AProtocol: TgoMongoProtocol; AReadPreference: tgoMongoReadPreference)
  : igoMongoCursor;
var
  Cursor: TgoBsonDocument;
  Value: TgoBsonValue;
  I: Integer;
  CursorID: Int64;
  Namespace: string;
  Docs: TgoBsonArray;
  InitialPage: TArray<TBytes>;
  icursor: igoMongoCursor;
begin
  Result := nil;
  if not ADoc.IsNil then
  begin
    if HasCursor(ADoc, Cursor, CursorID, Namespace) then
    begin
      if (Cursor.TryGetValue('firstBatch', Value)) then
      begin
        Docs := Value.AsBsonArray; // tgoBSONArray
        SetLength(InitialPage, Docs.Count);
        for I := 0 to Docs.Count - 1 do
          InitialPage[I] := Docs[I].AsBsonDocument.ToBson;
        icursor := TgoMongoCursor.Create(AProtocol, AReadPreference, Namespace, InitialPage, CursorID);
        Result := icursor;
      end;
    end
    else // Just return a cursor with this one document
    begin
      SetLength(InitialPage, 1);
      InitialPage[0] := ADoc.ToBson;
      icursor := TgoMongoCursor.Create(AProtocol, AReadPreference, 'null.null', InitialPage, 0);
      Result := icursor;
    end;
  end;
end;

function ExhaustCursor(const aCursor: igoMongoCursor): TArray<TgoBsonDocument>;
begin
  Result := nil;
  if assigned(aCursor) then
    Result := aCursor.ToArray;
end;

type
  { Implements IgoMongoCollection }
  TgoMongoCollection = class(TInterfacedObject, IgoMongoCollection)
{$REGION 'Internal Declarations'}
  private type
    PgoBsonDocument = ^TgoBsonDocument;
  private
    FDatabase: IgoMongoDatabase;
    FProtocol: TgoMongoProtocol; // Reference
    FName: string;
    FReadPreference: tgoMongoReadPreference;
  private
    procedure AddWriteConcern(const AWriter: IgoBsonWriter);
    procedure SpecifyDB(const AWriter: IgoBsonWriter);
    procedure SpecifyReadPreference(const AWriter: IgoBsonWriter);

    function InsertMany(const ADocuments: PgoBsonDocument; const ACount: Integer; const AOrdered: Boolean): Integer; overload;
    function Delete(const AFilter: TgoMongoFilter; const AOrdered: Boolean; const ALimit: Integer): Integer;
    function Update(const AFilter: TgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert, AOrdered, AMulti: Boolean): Integer;
    function GetReadPreference: tgoMongoReadPreference;
    procedure SetReadPreference(const Value: tgoMongoReadPreference);
  protected
    { IgoMongoCollection }
    function _GetDatabase: IgoMongoDatabase;
    function _GetName: string;

    function InsertOne(const ADocument: TgoBsonDocument): Boolean;
    function InsertMany(const ADocuments: array of TgoBsonDocument; const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TArray<TgoBsonDocument>; const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TEnumerable<TgoBsonDocument>; const AOrdered: Boolean = True): Integer; overload;

    function DeleteOne(const AFilter: TgoMongoFilter): Boolean;
    function DeleteMany(const AFilter: TgoMongoFilter; const AOrdered: Boolean = True): Integer;

    function UpdateOne(const AFilter: TgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert: Boolean = false): Boolean;
    function UpdateMany(const AFilter: TgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert: Boolean = false;
      const AOrdered: Boolean = True): Integer;

    function Find(const AFilter: TgoMongoFilter; const AProjection: TgoMongoProjection): igoMongoCursor; overload;
    function Find(const AFilter: TgoMongoFilter): igoMongoCursor; overload;
    function Find(const AProjection: TgoMongoProjection): igoMongoCursor; overload;
    function Find: igoMongoCursor; overload;
    function Find(const AFilter: TgoMongoFilter; const ASort: TgoMongoSort): igoMongoCursor; overload;
    function Find(const AFilter: TgoMongoFilter; const AProjection: TgoMongoProjection; const ASort: TgoMongoSort;
      const ANumberToSkip: Integer = 0): igoMongoCursor; overload;
    function FindOne(const AFilter: TgoMongoFilter; const AProjection: TgoMongoProjection): TgoBsonDocument; overload;
    function FindOne(const AFilter: TgoMongoFilter): TgoBsonDocument; overload;
    function FindOne(const AFilter: TgoMongoFilter; const ASort: TgoMongoSort): TgoBsonDocument; overload;
    function FindOne(const AFilter: TgoMongoFilter; const AProjection: TgoMongoProjection; const ASort: TgoMongoSort)
      : TgoBsonDocument; overload;

    function Count: Integer; overload;
    function Count(const AFilter: TgoMongoFilter): Integer; overload;

    function CreateIndex(const AName: string; const AKeyFields: array of string; const AUnique: Boolean = false): Boolean;
    function CreateTextIndex(const AName: string; const AFields: array of string; const ALanguageOverwriteField: string = '';
      const ADefaultLanguage: string = 'en'): Boolean;
    function DropIndex(const AName: string): Boolean;
    function ListIndexNames: TArray<string>;
    function ListIndexes: TArray<TgoBsonDocument>;
    function Stats: TgoBsonDocument;
{$ENDREGION 'Internal Declarations'}
  public
    property ReadPreference: tgoMongoReadPreference read GetReadPreference write SetReadPreference;
    constructor Create(const ADatabase: TgoMongoDatabase; const AName: string);
  end;

  { EgoMongoDBWriteError }

constructor EgoMongoDBWriteError.Create(const AErrorCode: TgoMongoErrorCode; const AErrorMsg: string);
begin
  inherited Create(AErrorMsg + Format(' (error %d)', [Ord(AErrorCode)]));
  FErrorCode := AErrorCode;
end;

{ TgoMongoClientSettings }

class function TgoMongoClientSettings.Create: TgoMongoClientSettings;
begin
  Result.ConnectionTimeout := 5000;
  Result.ReplyTimeout := 5000;
  Result.QueryFlags := [];
  Result.Secure := false;
  Result.Certificate := nil;
  Result.PrivateKey := nil;
  Result.PrivateKeyPassword := '';
  Result.AuthMechanism := TgoMongoAuthMechanism.None;
  Result.AuthDatabase := '';
  Result.Username := '';
  Result.Password := '';
  Result.GlobalReadPreference := tgoMongoReadPreference.Primary;
end;

{ TgoMongoClient }

constructor TgoMongoClient.Create(const AHost: string; const APort: Integer);
begin
  Create(AHost, APort, TgoMongoClientSettings.Create);
end;

constructor TgoMongoClient.Create(const AHost: string; const APort: Integer; const ASettings: TgoMongoClientSettings);
var
  S: TgoMongoProtocolSettings;
begin
  inherited Create;
  S.GlobalReadPreference := ASettings.GlobalReadPreference;
  if S.GlobalReadPreference = tgoMongoReadPreference.fromParent then
    S.GlobalReadPreference := tgoMongoReadPreference.Primary;
  S.ConnectionTimeout := ASettings.ConnectionTimeout;
  S.ReplyTimeout := ASettings.ReplyTimeout;
  S.QueryFlags := ASettings.QueryFlags;
  S.Secure := ASettings.Secure;
  S.Certificate := ASettings.Certificate;
  S.PrivateKey := ASettings.PrivateKey;
  S.PrivateKeyPassword := ASettings.PrivateKeyPassword;
  S.AuthMechanism := ASettings.AuthMechanism;
  S.AuthDatabase := ASettings.AuthDatabase;
  S.Username := ASettings.Username;
  S.Password := ASettings.Password;
  FProtocol := TgoMongoProtocol.Create(AHost, APort, S);
end;

constructor TgoMongoClient.Create(const ASettings: TgoMongoClientSettings);
begin
  Create(DEFAULT_HOST, DEFAULT_PORT, ASettings);
end;

destructor TgoMongoClient.Destroy;
begin
  FProtocol.Free;
  inherited;
end;

procedure TgoMongoClient.DropDatabase(const AName: string);
// https://docs.mongodb.com/manual/reference/command/dropDatabase/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('dropDatabase', 1);
  Writer.WriteString('$db', AName);
  { TODO : Readpreference??? }
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  HandleCommandReply(Reply);
end;

function TgoMongoClient.GetDatabase(const AName: string): IgoMongoDatabase;
begin
  Result := TgoMongoDatabase.Create(Self, AName);
end;

function TgoMongoClient.ListDatabaseNames: TArray<string>;
var
  Docs: TArray<TgoBsonDocument>;
  I: Integer;
begin
  Docs := ListDatabases;
  SetLength(Result, Length(Docs));
  for I := 0 to Length(Docs) - 1 do
    Result[I] := Docs[I]['name'];
end;

function TgoMongoClient.ListDatabases: TArray<TgoBsonDocument>;
// https://docs.mongodb.com/manual/reference/command/listDatabases/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  Doc: TgoBsonDocument;
  Databases: TgoBsonArray;
  Value: TgoBsonValue;
  I: Integer;
begin
  Result := nil;
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('listDatabases', 1);
  Writer.WriteString('$db', DB_ADMIN);
  { TODO : Readpreference??? }
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  HandleCommandReply(Reply);
  Doc := Reply.FirstDoc;
  if not Doc.IsNil then
  begin
    if (Doc.TryGetValue('databases', Value)) then
    begin
      Databases := Value.AsBsonArray;
      SetLength(Result, Databases.Count);
      for I := 0 to Databases.Count - 1 do
        Result[I] := Databases[I].AsBsonDocument;
    end;
  end;
end;

function TgoMongoClient.GetInstanceInfo(const ASaslSupportedMechs: string = ''; const AComment: string = ''): TgoMongoInstanceInfo;
// https://docs.mongodb.com/manual/reference/command/isMaster/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  Doc: TgoBsonDocument;
  InstArray: TgoBsonArray;
  I: Integer;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('isMaster', 1);
  Writer.WriteString('$db', DB_ADMIN);
  if (Length(ASaslSupportedMechs) > 0) then
  begin
    Writer.WriteString('saslSupportedMechs', ASaslSupportedMechs);
    if (Length(AComment) > 0) then
      Writer.WriteString('Comment', AComment);
  end;
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  HandleCommandReply(Reply);

  Doc := Reply.FirstDoc;
  if not(Doc.IsNil) then
  begin
    Result.Primary := TgoMongoInstance.Create(Doc.Get('primary', '').ToString);
    Result.Me := TgoMongoInstance.Create(Doc.Get('me', '').ToString);
    Result.SetName := Doc.Get('setName', '').ToString;
    Result.SetVersion := Doc.Get('setVersion', 0).ToInteger;
    Result.IsMaster := Doc.Get('ismaster', false).ToBoolean;
    Result.IsSecondary := Doc.Get('secondary', false).ToBoolean;
    Result.ArbiterOnly := Doc.Get('arbiterOnly', false).ToBoolean;
    Result.LocalTime := Doc.Get('localTime', 0).ToUniversalTime;
    Result.ConnectionId := Doc.Get('connectionId', 0).ToInteger;
    Result.ReadOnly := Doc.Get('readOnly', True).ToBoolean;

    if Doc.Contains('hosts') then
    begin
      InstArray := Doc.Get('hosts', '').AsBsonArray;
      SetLength(Result.Hosts, InstArray.Count);
      for I := 0 to InstArray.Count - 1 do
        Result.Hosts[I] := TgoMongoInstance.Create(InstArray.Items[I].ToString);
    end
    else
      Result.Hosts := nil;

    if Doc.Contains('arbiters') then
    begin
      InstArray := Doc.Get('arbiters', '').AsBsonArray;
      SetLength(Result.Arbiters, InstArray.Count);
      for I := 0 to InstArray.Count - 1 do
        Result.Arbiters[I] := TgoMongoInstance.Create(InstArray.Items[I].ToString);
    end
    else
      Result.Arbiters := nil;
  end
  else
    raise Exception.Create('invalid response');
end;

function TgoMongoClient.IsMaster: Boolean;
begin
  Result := Self.GetInstanceInfo().IsMaster;
end;

{ This method performs an administrative command and returns ONE document.
  It uses dependency injection by calling an anonymous method that "injects"
  commands into the BSON document }

function TgoMongoClient.AdminCommand(CommandToIssue: tWriteCmd): igoMongoCursor;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  CommandToIssue(Writer); // let the anonymous method write the commands
  Writer.WriteString('$db', DB_ADMIN);
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil);
  HandleCommandReply(Reply);
  Result := firstBatchToCursor(Reply.FirstDoc, FProtocol, FProtocol.GlobalReadPreference);
end;

function TgoMongoClient.BuildInfo: TgoBsonDocument;
var
  Doc: TgoBsonDocument;
begin
  Result.SetNil;
  for Doc in AdminCommand(
    procedure(Writer: IgoBsonWriter)
    begin
      Writer.WriteInt32('buildInfo', 1);
    end) do
    Result := Doc;
end;

function TgoMongoClient.Features: TgoBsonDocument;
var
  Doc: TgoBsonDocument;
begin
  Result.SetNil;
  for Doc in AdminCommand(
    procedure(Writer: IgoBsonWriter)
    begin
      Writer.WriteInt32('features', 1);
    end) do
    Result := Doc;
end;

function TgoMongoClient.GetGlobalReadPreference: tgoMongoReadPreference;
begin
  Result := FProtocol.GlobalReadPreference;
end;

function TgoMongoClient.Hello: TgoBsonDocument;
var
  Doc: TgoBsonDocument;
begin
  Result.SetNil;
  for Doc in AdminCommand(
    procedure(Writer: IgoBsonWriter)
    begin
      Writer.WriteInt32('hello', 1);
    end) do
    Result := Doc;
end;

function TgoMongoClient.HostInfo: TgoBsonDocument;
var
  Doc: TgoBsonDocument;
begin
  Result.SetNil;
  for Doc in AdminCommand(
    procedure(Writer: IgoBsonWriter)
    begin
      Writer.WriteInt32('hostInfo', 1);
    end) do
    Result := Doc;
end;

function TgoMongoClient.LogRotate: Boolean;
var
  Doc: TgoBsonDocument;
begin
  Result := false;
  for Doc in AdminCommand(
    procedure(Writer: IgoBsonWriter)
    begin
      Writer.WriteInt32('logRotate', 1);
    end) do
    if not Doc.IsNil then
      Result := Doc['ok']
end;

procedure TgoMongoClient.SetGlobalReadPreference(const Value: tgoMongoReadPreference);
begin
  FProtocol.GlobalReadPreference := Value;
end;

{ TgoMongoDatabase }

procedure TgoMongoDatabase.SpecifyDB(const AWriter: IgoBsonWriter);
begin
  AWriter.WriteString('$db', name);
end;

procedure TgoMongoDatabase.SpecifyReadPreference(const AWriter: IgoBsonWriter);
begin
  DoSpecifyReadPreference(GetReadPreference, AWriter);
end;

constructor TgoMongoDatabase.Create(const AClient: TgoMongoClient; const AName: string);
begin
  Assert(AClient <> nil);
  Assert(AName <> '');
  inherited Create;
  FClient := AClient;
  FName := AName;
  FProtocol := AClient.Protocol;
  Assert(FProtocol <> nil);
  FReadPreference := tgoMongoReadPreference.fromParent;
end;

function TgoMongoDatabase.AdminCommand(CommandToIssue: tWriteCmd): igoMongoCursor;
begin
  Result := FClient.AdminCommand(CommandToIssue);
end;

function TgoMongoDatabase.Command(CommandToIssue: tWriteCmd): igoMongoCursor;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  CommandToIssue(Writer); // let the anonymous method write the commands
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteEndDocument;
  Reply := Protocol.OpMsg(Writer.ToBson, nil);
  HandleCommandReply(Reply);
  Result := firstBatchToCursor(Reply.FirstDoc, FProtocol, GetReadPreference);
end;

procedure TgoMongoDatabase.DropCollection(const AName: string);
// https://docs.mongodb.com/manual/reference/command/drop/#dbcmd.drop
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('drop', AName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  HandleCommandReply(Reply, TgoMongoErrorCode.NamespaceNotFound);
end;

function TgoMongoDatabase.GetCollection(const AName: string): IgoMongoCollection;
begin
  Result := TgoMongoCollection.Create(Self, AName);
end;

function TgoMongoDatabase.GetDbStats(const AScale: Integer): TgoMongoStatistics;
// https://docs.mongodb.com/manual/reference/command/dbStats/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  Doc: TgoBsonDocument;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('dbStats', 1);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteInt32('scale', AScale);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  HandleCommandReply(Reply);
  Doc := Reply.FirstDoc;
  if Doc.IsNil then
    raise EgoMongoDBError.Create(RS_MONGODB_GENERIC_ERROR);

  Result.Database := Doc.Get('db', '').ToString;
  Result.Collections := Doc.Get('collections', 0).ToInteger;
  Result.Views := Doc.Get('views', 0).ToInteger;
  Result.Objects := Doc.Get('objects', 0).ToInt64;
  Result.AvgObjSize := Doc.Get('avgObjSize', 0).ToDouble;
  Result.DataSize := Doc.Get('dataSize', 0).ToDouble;
  Result.StorageSize := Doc.Get('storageSize', 0).ToDouble;
  Result.NumExtents := Doc.Get('numExtents', 0).ToInteger;
  Result.Indexes := Doc.Get('indexes', 0).ToInteger;
  Result.IndexSize := Doc.Get('indexSize', 0).ToDouble;
  Result.ScaleFactor := Doc.Get('scaleFactor', 0).ToDouble;
  Result.FsUsedSize := Doc.Get('fsUsedSize', 0).ToDouble;
  Result.FsTotalSize := Doc.Get('fsTotalSize', 0).ToDouble;
end;

function TgoMongoDatabase.CreateCollection(const AName: string; const ACapped: Boolean; const AMaxSize, AMaxDocuments: Int64;
const AValidationLevel: TgoMongoValidationLevel; const AValidationAction: TgoMongoValidationAction; const AValidator: TgoBsonDocument;
const ACollation: TgoMongoCollation): Boolean;
// https://docs.mongodb.com/manual/reference/method/db.createCollection/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('create', AName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);

  Writer.WriteBoolean('capped', ACapped);
  if ACapped = True then
  begin
    Writer.WriteInt64('size', AMaxSize);
    Writer.WriteInt64('max', AMaxDocuments);
  end;
  // timeSeries...

  if AValidator.IsNil = false then
  begin
    Writer.WriteName('validator');
    Writer.WriteRawBsonDocument(AValidator.ToBson);
    Writer.WriteString('validationLevel', AValidationLevel.ToString);
    Writer.WriteString('validationAction', AValidationAction.ToString);
  end;

  Writer.WriteName('collation');
  Writer.WriteStartDocument;
  Writer.WriteString('locale', ACollation.Locale);
  Writer.WriteBoolean('caseLevel', ACollation.CaseLevel);
  Writer.WriteString('caseFirst', ACollation.CaseFirst.ToString);
  Writer.WriteInt32('strength', ACollation.Strength);
  Writer.WriteBoolean('numericOrdering', ACollation.NumericOrdering);
  Writer.WriteString('alternate', ACollation.Alternate.ToString);
  Writer.WriteString('maxVariable', ACollation.MaxVariable.ToString);
  Writer.WriteBoolean('backwards', ACollation.Backwards);
  Writer.WriteEndDocument;
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  Result := (HandleCommandReply(Reply) = 0);
end;

function TgoMongoDatabase.GetReadPreference: tgoMongoReadPreference;
begin
  Result := FReadPreference;
  if Result = tgoMongoReadPreference.fromParent then
    Result := FProtocol.GlobalReadPreference;
end;

function TgoMongoDatabase.ListCollectionNames: TArray<string>;
var
  Docs: TArray<TgoBsonDocument>;
  I: Integer;
begin
  Docs := ListCollections;
  SetLength(Result, Length(Docs));
  for I := 0 to Length(Docs) - 1 do
    Result[I] := Docs[I]['name'];
end;

function TgoMongoDatabase.ListCollections: TArray<TgoBsonDocument>;
// https://docs.mongodb.com/manual/reference/command/listCollections/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Result := nil;
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('listCollections', 1);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  HandleCommandReply(Reply);
  Result := ExhaustCursor(firstBatchToCursor(Reply.FirstDoc, FProtocol, GetReadPreference));
end;

function TgoMongoDatabase.RenameCollection(const AFromNamespace, AToNamespace: string; const ADropTarget: Boolean): Boolean;
// https://docs.mongodb.com/manual/reference/command/renameCollection/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('renameCollection', AFromNamespace);
  Writer.WriteString('to', AToNamespace);
  Writer.WriteBoolean('dropTarget', ADropTarget);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  Result := (HandleCommandReply(Reply) = 0);
end;

procedure TgoMongoDatabase.SetReadPreference(const Value: tgoMongoReadPreference);
begin
  FReadPreference := Value;
end;

function TgoMongoDatabase._GetClient: IgoMongoClient;
begin
  Result := FClient;
end;

function TgoMongoDatabase._GetName: string;
begin
  Result := FName;
end;

{ TgoMongoCursor }

constructor TgoMongoCursor.Create(const AProtocol: TgoMongoProtocol; AReadPreference: tgoMongoReadPreference;
const ADatabaseName, ACollectionName: string; const AInitialPage: TArray<TBytes>; const AInitialCursorId: Int64);
begin
  inherited Create;
  FProtocol := AProtocol;
  FDatabaseName := ADatabaseName;
  FCollectionName := ACollectionName;
  FInitialPage := AInitialPage;
  FInitialCursorId := AInitialCursorId;
  FReadPreference := AReadPreference;
end;

constructor TgoMongoCursor.Create(const AProtocol: TgoMongoProtocol; AReadPreference: tgoMongoReadPreference; const aNameSpace: string;
const AInitialPage: TArray<TBytes>; const AInitialCursorId: Int64);
var
  dotpos: Integer;
begin
  inherited Create;
  dotpos := Pos('.', aNameSpace);
  FProtocol := AProtocol;
  FDatabaseName := copy(aNameSpace, 1, dotpos - 1);
  FCollectionName := copy(aNameSpace, dotpos + 1, Length(aNameSpace));
  FInitialPage := AInitialPage;
  FInitialCursorId := AInitialCursorId;
  FReadPreference := AReadPreference;
end;

function TgoMongoCursor.GetEnumerator: TEnumerator<TgoBsonDocument>;
begin
  Result := TEnumerator.Create(FProtocol, FReadPreference, FDatabaseName, FCollectionName, FInitialPage, FInitialCursorId);
end;

function TgoMongoCursor.ToArray: TArray<TgoBsonDocument>;
var
  Count, Capacity: Integer;
  Doc: TgoBsonDocument;
begin
  Count := 0;
  Capacity := 16;
  SetLength(Result, Capacity);
  for Doc in Self do
  begin
    if (Count >= Capacity) then
    begin
      Capacity := Capacity * 2;
      SetLength(Result, Capacity);
    end;
    Result[Count] := Doc;
    Inc(Count);
  end;

  SetLength(Result, Count);
end;

{ TgoMongoCursor.TEnumerator }

procedure TgoMongoCursor.TEnumerator.SpecifyDB(const Writer: IgoBsonWriter);
begin
  Writer.WriteString('$db', FDatabaseName);
end;

procedure TgoMongoCursor.TEnumerator.SpecifyReadPreference(const AWriter: IgoBsonWriter);
begin
  DoSpecifyReadPreference(FReadPreference, AWriter);
end;

constructor TgoMongoCursor.TEnumerator.Create(const AProtocol: TgoMongoProtocol; AReadPreference: tgoMongoReadPreference;
const ADatabaseName, ACollectionName: string; const APage: TArray<TBytes>; const ACursorId: Int64);
begin
  inherited Create;
  FProtocol := AProtocol;
  FDatabaseName := ADatabaseName;
  FCollectionName := ACollectionName;
  FPage := APage;
  FCursorId := ACursorId;
  FReadPreference := AReadPreference;
  FIndex := -1;
end;

destructor TgoMongoCursor.TEnumerator.Destroy;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  if FCursorId <> 0 then // we exited the for...in loop before the cursor was exhausted
  begin
    try
      Writer := TgoBsonWriter.Create;
      Writer.WriteStartDocument;
      Writer.WriteString('killCursors', FCollectionName);
      Writer.WriteStartArray('cursors');
      Writer.WriteInt64(FCursorId);
      Writer.WriteEndArray;
      SpecifyDB(Writer);
      SpecifyReadPreference(Writer);
      Writer.WriteEndDocument;
      { "true" tells protocol to NOT expect a result - saves one roundtrip }
      Reply := FProtocol.OpMsg(Writer.ToBson, nil, True);
    except
      // always ignore exceptions in a destructor!
    end;
  end;
  inherited;
end;

function TgoMongoCursor.TEnumerator.DoGetCurrent: TgoBsonDocument;
begin
  Result := TgoBsonDocument.Load(FPage[FIndex]);
end;

function TgoMongoCursor.TEnumerator.DoMoveNext: Boolean;
begin
  Result := (FIndex < (Length(FPage) - 1));
  if Result then
    Inc(FIndex)
  else if (FCursorId <> 0) then
  begin
    { Get next page from server.
      Note: if FCursorId = 0, then all documents did fit in the reply, so there
      is no need to get more data from the server. }
    GetMore;
    Result := (FPage <> nil);
  end;
end;

procedure TgoMongoCursor.TEnumerator.GetMore;
var
  Reply: IgoMongoReply;
  Writer: IgoBsonWriter;
  ADoc, Cursor: TgoBsonDocument;
  Docs: TgoBsonArray;
  Value: TgoBsonValue;
  I: Integer;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt64('getMore', FCursorId);
  Writer.WriteString('collection', FCollectionName);
  Writer.WriteInt32('batchSize', Length(FPage));
  { MaxTimeMS }
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  HandleTimeout(Reply);
  FIndex := 0;
  SetLength(FPage, 0);
  ADoc := Reply.FirstDoc;
  if not ADoc.IsNil then
  begin
    if ADoc.Contains('cursor') then
    begin
      Cursor := ADoc['cursor'].AsBsonDocument;
      // The cursor ID should become 0 when it is exhausted
      FCursorId := Cursor['id']; // less overhead to do it here, than query reply.cursorid
      // Namespace:=Cursor.Get('ns','').ToString();   --> does not change
      Docs := Cursor['nextBatch'].AsBsonArray;
      SetLength(FPage, Docs.Count);
      I := 0;
      for Value in Docs do
      begin
        FPage[I] := Value.AsBsonDocument.ToBson;
        Inc(I);
      end;
    end;
  end;
end;

{ TgoMongoCollection }

procedure TgoMongoCollection.SetReadPreference(const Value: tgoMongoReadPreference);
begin
  FReadPreference := Value;
end;

procedure TgoMongoCollection.SpecifyDB(const AWriter: IgoBsonWriter);
begin
  AWriter.WriteString('$db', FDatabase.name);
end;

procedure TgoMongoCollection.SpecifyReadPreference(const AWriter: IgoBsonWriter);
begin
  DoSpecifyReadPreference(GetReadPreference, AWriter);
end;

procedure TgoMongoCollection.AddWriteConcern(const AWriter: IgoBsonWriter);
begin
  { Write concerns are currently not supported }
end;

function TgoMongoCollection.Count: Integer;
begin
  Result := Count(TgoMongoFilter.Empty);
end;

function TgoMongoCollection.Count(const AFilter: TgoMongoFilter): Integer;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('count', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteName('query');
  Writer.WriteRawBsonDocument(AFilter.ToBson);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  Result := HandleCommandReply(Reply);
end;

constructor TgoMongoCollection.Create(const ADatabase: TgoMongoDatabase; const AName: string);
begin
  Assert(assigned(ADatabase));
  Assert(AName <> '');
  inherited Create;
  FDatabase := ADatabase;
  FName := AName;
  FProtocol := ADatabase.Protocol;
  FReadPreference := tgoMongoReadPreference.fromParent;
  Assert(FProtocol <> nil);
end;

function TgoMongoCollection.CreateIndex(const AName: string; const AKeyFields: array of string; const AUnique: Boolean = false): Boolean;
// https://docs.mongodb.com/manual/reference/command/createIndexes/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  I: Integer;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('createIndexes', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);

  Writer.WriteStartArray('indexes');
  Writer.WriteStartDocument;
  Writer.WriteStartDocument('key');
  for I := 0 to high(AKeyFields) do
    Writer.WriteInt32(AKeyFields[I], 1);
  Writer.WriteEndDocument;
  Writer.WriteString('name', AName);
  Writer.WriteBoolean('unique', AUnique);
  Writer.WriteEndDocument;
  Writer.WriteEndArray;
  AddWriteConcern(Writer);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  Result := (HandleCommandReply(Reply) = 0);
end;

function TgoMongoCollection.CreateTextIndex(const AName: string; const AFields: array of string; const ALanguageOverwriteField: string = '';
const ADefaultLanguage: string = 'en'): Boolean;
// https://docs.mongodb.com/manual/core/index-text/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  I: Integer;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('createIndexes', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteStartArray('indexes');
  Writer.WriteStartDocument;
  Writer.WriteStartDocument('key');
  for I := 0 to high(AFields) do
    Writer.WriteString(AFields[I], 'text');
  Writer.WriteEndDocument;
  Writer.WriteString('name', AName);

  if ADefaultLanguage.IsEmpty = false then
    Writer.WriteString('default_language', ADefaultLanguage);

  if ALanguageOverwriteField.IsEmpty = false then
    Writer.WriteString('language_override', ALanguageOverwriteField);
  Writer.WriteEndDocument;
  Writer.WriteEndArray;
  AddWriteConcern(Writer);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  Result := (HandleCommandReply(Reply) = 0);
end;

function TgoMongoCollection.DropIndex(const AName: string): Boolean;
// https://docs.mongodb.com/manual/reference/command/dropIndexes/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('dropIndexes', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteString('index', AName);
  AddWriteConcern(Writer);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  Result := (HandleCommandReply(Reply) = 0);
end;

function TgoMongoCollection.ListIndexNames: TArray<string>;
// https://docs.mongodb.com/manual/reference/command/listIndexes/
var
  Docs: TArray<TgoBsonDocument>;
  I: Integer;
begin
  Docs := ListIndexes;
  SetLength(Result, Length(Docs));
  for I := 0 to Length(Docs) - 1 do
    Result[I] := Docs[I]['name'];
end;

function TgoMongoCollection.Stats: TgoBsonDocument;
// https://www.mongodb.com/docs/manual/reference/command/collStats/
var
  Doc: TgoBsonDocument;
begin
  Doc.SetNil;
  for Doc in FDatabase.Command(
    procedure(Writer: IgoBsonWriter)
    begin
      Writer.WriteString('collStats', FName);
    end) do;
  Result := Doc;
end;

function TgoMongoCollection.ListIndexes: TArray<TgoBsonDocument>;
// https://docs.mongodb.com/manual/reference/command/listIndexes/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Result := nil;
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('listIndexes', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  HandleCommandReply(Reply);
  Result := ExhaustCursor(firstBatchToCursor(Reply.FirstDoc, FProtocol, GetReadPreference));
end;

function TgoMongoCollection.Delete(const AFilter: TgoMongoFilter; const AOrdered: Boolean; const ALimit: Integer): Integer;
// https://docs.mongodb.com/manual/reference/command/delete/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('delete', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteStartArray('deletes');
  Writer.WriteStartDocument;
  Writer.WriteName('q');
  Writer.WriteRawBsonDocument(AFilter.ToBson);
  Writer.WriteInt32('limit', ALimit);
  Writer.WriteEndDocument;
  Writer.WriteEndArray;
  AddWriteConcern(Writer);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  Result := HandleCommandReply(Reply);
end;

function TgoMongoCollection.DeleteMany(const AFilter: TgoMongoFilter; const AOrdered: Boolean): Integer;
begin
  Result := Delete(AFilter, AOrdered, 0);
end;

function TgoMongoCollection.DeleteOne(const AFilter: TgoMongoFilter): Boolean;
begin
  Result := (Delete(AFilter, True, 1) = 1);
end;

function TgoMongoCollection.Find(const AFilter: TgoMongoFilter): igoMongoCursor;
var
  Projection: TgoMongoProjection; // record
  Sort: TgoMongoSort; // record
begin
  Projection.SetNil;
  Sort.SetNil;
  Result := Find(AFilter, Projection, Sort);
end;

function TgoMongoCollection.Find(const AFilter: TgoMongoFilter; const AProjection: TgoMongoProjection): igoMongoCursor;
var
  Sort: TgoMongoSort; // record
begin
  Sort.SetNil;
  Result := Find(AFilter, AProjection, Sort);
end;

function TgoMongoCollection.Find: igoMongoCursor;
var
  Projection: TgoMongoProjection; // record
  Sort: TgoMongoSort; // record
  Filter: TgoMongoFilter; // record
begin
  Projection.SetNil;
  Sort.SetNil;
  Filter := TgoMongoFilter.Empty;
  Result := Find(Filter, Projection, Sort);
end;

function TgoMongoCollection.Find(const AProjection: TgoMongoProjection): igoMongoCursor;
var
  Sort: TgoMongoSort;
  Filter: TgoMongoFilter;
begin
  Sort.SetNil;
  Filter := TgoMongoFilter.Empty;
  Result := Find(Filter, AProjection, Sort);
end;

function TgoMongoCollection.Find(const AFilter: TgoMongoFilter; const ASort: TgoMongoSort): igoMongoCursor;
var
  Projection: TgoMongoProjection;
begin
  Projection.SetNil;
  Result := Find(AFilter, Projection, ASort);
end;

// https://docs.mongodb.com/manual/reference/method/db.collection.find
function TgoMongoCollection.Find(const AFilter: TgoMongoFilter; const AProjection: TgoMongoProjection; const ASort: TgoMongoSort;
const ANumberToSkip: Integer = 0): igoMongoCursor;
var
  Reply: IgoMongoReply;
  Writer: IgoBsonWriter;
begin
  Result := nil;

  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('find', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);

  if not AFilter.IsNil then
  begin
    Writer.WriteName('filter');
    Writer.WriteRawBsonDocument(AFilter.ToBson);
  end;
  if not ASort.IsNil then
  begin
    Writer.WriteName('sort');
    Writer.WriteRawBsonDocument(ASort.ToBson);
  end;
  if not AProjection.IsNil then
  begin
    Writer.WriteName('projection');
    Writer.WriteRawBsonDocument(AProjection.ToBson);
  end;
  Writer.WriteInt32('skip', ANumberToSkip);
  Writer.WriteEndDocument;

  // outputdebugstring(pchar(tgobsondocument.Load(writer.tobson).ToJson));

  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  HandleCommandReply(Reply);
  Result := firstBatchToCursor(Reply.FirstDoc, FProtocol, GetReadPreference);
end;

function TgoMongoCollection.FindOne(const AFilter: TgoMongoFilter; const AProjection: TgoMongoProjection; const ASort: TgoMongoSort)
  : TgoBsonDocument;
var
  Reply: IgoMongoReply;
  Writer: IgoBsonWriter;
  Doc, Cursor: TgoBsonDocument;
  Value: TgoBsonValue;
  Docs: TgoBsonArray;
begin
  // Important info here !!!!!
  // https://www.mongodb.com/docs/manual/reference/command/find/

  Result.SetNil;
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('find', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  if not AFilter.IsNil then
  begin
    Writer.WriteName('filter');
    Writer.WriteRawBsonDocument(AFilter.ToBson);
  end;
  if not ASort.IsNil then
  begin
    Writer.WriteName('sort');
    Writer.WriteRawBsonDocument(ASort.ToBson);
  end;
  if not AProjection.IsNil then
  begin
    Writer.WriteName('projection');
    Writer.WriteRawBsonDocument(AProjection.ToBson);
  end;
  Writer.WriteInt32('limit', 1); // limit total result set to 1
  // Writer.WriteInt32('batchSize', 1); // limit batch size to 1 (default is 101) --> redundant
  Writer.WriteBoolean('singleBatch', True); // close cursor after first batch

  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  HandleCommandReply(Reply);
  Doc := Reply.FirstDoc;
  if not Doc.IsNil then
  begin
    if Doc.TryGetValue('cursor', Value) then
    begin
      Cursor := Value.AsBsonDocument;
      // we don't need the cursor ID because it will be 0
      // FCursorId := cursor['id'];
      if Cursor.TryGetValue('firstBatch', Value) then
      begin
        Docs := Value.AsBsonArray;
        if Docs.Count > 0 then // need just the first one
          Result := Docs[0].AsBsonDocument;
      end;
    end;
  end;
end;

function TgoMongoCollection.GetReadPreference: tgoMongoReadPreference;
begin
  Result := FReadPreference;
  if Result = tgoMongoReadPreference.fromParent then
    Result := FDatabase.ReadPreference;
end;

function TgoMongoCollection.FindOne(const AFilter: TgoMongoFilter; const AProjection: TgoMongoProjection): TgoBsonDocument;
var
  Sort: TgoMongoSort; // record
begin
  Sort.SetNil;
  Result := FindOne(AFilter, AProjection, Sort);
end;

function TgoMongoCollection.FindOne(const AFilter: TgoMongoFilter): TgoBsonDocument;
var
  Projection: TgoMongoProjection; // record
  Sort: TgoMongoSort; // record
begin
  Projection.SetNil;
  Sort.SetNil;
  Result := FindOne(AFilter, Projection, Sort);
end;

function TgoMongoCollection.FindOne(const AFilter: TgoMongoFilter; const ASort: TgoMongoSort): TgoBsonDocument;
var
  Projection: TgoMongoProjection;
begin
  Projection.SetNil;
  Result := FindOne(AFilter, Projection, ASort);
end;

function TgoMongoCollection.InsertMany(const ADocuments: array of TgoBsonDocument; const AOrdered: Boolean): Integer;
begin
  if (Length(ADocuments) > 0) then
    Result := InsertMany(@ADocuments[0], Length(ADocuments), AOrdered)
  else
    Result := 0;
end;

function TgoMongoCollection.InsertMany(const ADocuments: TArray<TgoBsonDocument>; const AOrdered: Boolean): Integer;
begin
  if (Length(ADocuments) > 0) then
    Result := InsertMany(@ADocuments[0], Length(ADocuments), AOrdered)
  else
    Result := 0;
end;

function TgoMongoCollection.InsertMany(const ADocuments: PgoBsonDocument; const ACount: Integer; const AOrdered: Boolean): Integer;
// https://docs.mongodb.com/manual/reference/command/insert/#dbcmd.insert
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  I, Remaining, ItemsInBatch, Index, BytesEncoded: Integer;
  tb: TBytes;
  Payload0: TBytes;
  Payload1: TArray<tgoPayloadType1>;

begin
  Remaining := ACount;
  index := 0;
  Result := 0;
  BytesEncoded := 0;
  while (Remaining > 0) do
  begin
    ItemsInBatch := Min(Remaining, FProtocol.MaxWriteBatchSize);
    SetLength(Payload1, 0);
    Writer := TgoBsonWriter.Create;
    Writer.WriteStartDocument;
    Writer.WriteString('insert', FName);
    SpecifyDB(Writer);
    SpecifyReadPreference(Writer);

    (* DEPRECATED
      {This is the SLOW/LEGACY method because the server needs to unpack
      an array contained inside a very large document.
      It is faster to "outsource" the "documents" array into a separate
      sequence of Payload type 1}
      Writer.WriteStartArray('documents');
      for I := 0 to ItemsInBatch - 1 do
      begin
      Writer.WriteValue(ADocuments[index]);
      Inc(index);
      Dec(Remaining);
      end;
      Writer.WriteEndArray;
    *)

    Writer.WriteBoolean('ordered', AOrdered);
    AddWriteConcern(Writer);
    Writer.WriteEndDocument;
    Payload0 := Writer.ToBson;
    BytesEncoded := Length(Payload0) + 100; // overly generous estimation
    Writer := nil;

    { https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#command-arguments-as-payload
      "Bulk writes SHOULD use Payload Type 1, and MUST do so when the batch contains more than one entry."
      N.B.: This method is faster because the server can read the "documents" parameter as a
      simple sequential stream of small documents. }

    SetLength(Payload1, 1); // Send ONE sequence of Payload1 with multiple docs
    Payload1[0].name := 'documents';
    SetLength(Payload1[0].Docs, ItemsInBatch);
    for I := 0 to ItemsInBatch - 1 do
    begin
      tb := ADocuments[index].ToBson;
      { Avoid excessive message size or batch count }
      if ((BytesEncoded + Length(tb)) > FProtocol.MaxMessageSizeBytes) then
      begin
        SetLength(Payload1[0].Docs, I);
        Break;
      end;
      Inc(BytesEncoded, Length(tb));
      Payload1[0].Docs[I] := tb;
      Inc(index);
      dec(Remaining);
    end; // FOR

    Reply := FProtocol.OpMsg(Payload0, Payload1);
    Inc(Result, HandleCommandReply(Reply));
  end; // While
  Assert(index = ACount);
end;

function TgoMongoCollection.InsertMany(const ADocuments: TEnumerable<TgoBsonDocument>; const AOrdered: Boolean): Integer;
begin
  Result := InsertMany(ADocuments.ToArray, AOrdered);
end;

function TgoMongoCollection.InsertOne(const ADocument: TgoBsonDocument): Boolean;
// https://docs.mongodb.com/manual/reference/command/insert/#dbcmd.insert
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('insert', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteStartArray('documents');
  Writer.WriteValue(ADocument);
  Writer.WriteEndArray;
  AddWriteConcern(Writer);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  Result := (HandleCommandReply(Reply) = 1);
end;

function TgoMongoCollection.Update(const AFilter: TgoMongoFilter; const AUpdate: TgoMongoUpdate;
const AUpsert, AOrdered, AMulti: Boolean): Integer;
// https://docs.mongodb.com/manual/reference/command/update
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('update', FName);
  SpecifyDB(Writer);
  SpecifyReadPreference(Writer);
  Writer.WriteStartArray('updates');
  Writer.WriteStartDocument;
  Writer.WriteName('q');
  Writer.WriteRawBsonDocument(AFilter.ToBson);
  Writer.WriteName('u');
  Writer.WriteRawBsonDocument(AUpdate.ToBson);
  Writer.WriteBoolean('upsert', AUpsert);
  Writer.WriteBoolean('multi', AMulti);
  Writer.WriteEndDocument;
  Writer.WriteEndArray;
  Writer.WriteBoolean('ordered', AOrdered);
  AddWriteConcern(Writer);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpMsg(Writer.ToBson, nil);
  Result := HandleCommandReply(Reply);
end;

function TgoMongoCollection.UpdateMany(const AFilter: TgoMongoFilter; const AUpdate: TgoMongoUpdate;
const AUpsert, AOrdered: Boolean): Integer;
begin
  Result := Update(AFilter, AUpdate, AUpsert, AOrdered, True);
end;

function TgoMongoCollection.UpdateOne(const AFilter: TgoMongoFilter; const AUpdate: TgoMongoUpdate; const AUpsert: Boolean): Boolean;
begin
  Result := (Update(AFilter, AUpdate, AUpsert, false, false) = 1);
end;

function TgoMongoCollection._GetDatabase: IgoMongoDatabase;
begin
  Result := FDatabase;
end;

function TgoMongoCollection._GetName: string;
begin
  Result := FName;
end;

{ TgoMongoValidationLevelHelper }

function TgoMongoValidationLevelHelper.ToString: string;
begin
  case Self of
    TgoMongoValidationLevel.vlOff:
      Result := 'off';
    TgoMongoValidationLevel.vlStrict:
      Result := 'strict';
    TgoMongoValidationLevel.vlModerate:
      Result := 'moderate';
  else
    raise Exception.Create('invalid type');
  end;
end;

{ TgoMongoValidationActionHelper }

function TgoMongoValidationActionHelper.ToString: string;
begin
  case Self of
    TgoMongoValidationAction.vaError:
      Result := 'error';
    TgoMongoValidationAction.vaWarn:
      Result := 'warn';
  else
    raise Exception.Create('invalid type');
  end;
end;

{ TgoMongoCollationCaseFirstHelper }

function TgoMongoCollationCaseFirstHelper.ToString: string;
begin
  case Self of
    TgoMongoCollationCaseFirst.ccfUpper:
      Result := 'upper';
    TgoMongoCollationCaseFirst.ccfLower:
      Result := 'lower';
    TgoMongoCollationCaseFirst.ccfOff:
      Result := 'off';
  else
    raise Exception.Create('invalid type');
  end;
end;

{ TgoMongoCollationAlternateHelper }

function TgoMongoCollationAlternateHelper.ToString: string;
begin
  case Self of
    TgoMongoCollationAlternate.caNonIgnorable:
      Result := 'non-ignorable';
    TgoMongoCollationAlternate.caShifted:
      Result := 'shifted';
  else
    raise Exception.Create('invalid type');
  end;
end;

{ TgoMongoCollationMaxVariableHelper }

function TgoMongoCollationMaxVariableHelper.ToString: string;
begin
  case Self of
    TgoMongoCollationMaxVariable.cmvPunct:
      Result := 'punct';
    TgoMongoCollationMaxVariable.cmvSpace:
      Result := 'space';
  else
    raise Exception.Create('invalid type');
  end;
end;

{ TgoMongoInstance }

constructor TgoMongoInstance.Create(AHost: string; APort: Word);
begin
  Self.Host := AHost;
  Self.Port := APort;
end;

constructor TgoMongoInstance.Create(AInstance: string);
begin
  try
    if AInstance.Contains(':') = True then
    begin
      Self.Host := copy(AInstance, 1, Pos(':', AInstance) - 1).Trim;
      Self.Port := copy(AInstance, Pos(':', AInstance) + 1, AInstance.Length).Trim.ToInteger;
    end;
  except
    Self.Host := '';
    Self.Port := 0;
  end;
end;

end.
