unit Grijjy.MongoDB;
{< Main interface to MongoDB }

{$INCLUDE 'Grijjy.inc'}

interface

uses
  System.SysUtils,
  System.Generics.Collections,
  Grijjy.Bson,
  Grijjy.Bson.IO,
  Grijjy.MongoDB.Protocol,
  Grijjy.MongoDB.Queries;

type
  { MongoDB error codes }
  TgoMongoErrorCode = (
    OK = 0,
    InternalError = 1,
    BadValue = 2,
    OBSOLETE_DuplicateKey = 3,
    NoSuchKey = 4,
    GraphContainsCycle = 5,
    HostUnreachable = 6,
    HostNotFound = 7,
    UnknownError = 8,
    FailedToParse = 9,
    CannotMutateObject = 10,
    UserNotFound = 11,
    UnsupportedFormat = 12,
    Unauthorized = 13,
    TypeMismatch = 14,
    Overflow = 15,
    InvalidLength = 16,
    ProtocolError = 17,
    AuthenticationFailed = 18,
    CannotReuseObject = 19,
    IllegalOperation = 20,
    EmptyArrayOperation = 21,
    InvalidBSON = 22,
    AlreadyInitialized = 23,
    LockTimeout = 24,
    RemoteValidationError = 25,
    NamespaceNotFound = 26,
    IndexNotFound = 27,
    PathNotViable = 28,
    NonExistentPath = 29,
    InvalidPath = 30,
    RoleNotFound = 31,
    RolesNotRelated = 32,
    PrivilegeNotFound = 33,
    CannotBackfillArray = 34,
    UserModificationFailed = 35,
    RemoteChangeDetected = 36,
    FileRenameFailed = 37,
    FileNotOpen = 38,
    FileStreamFailed = 39,
    ConflictingUpdateOperators = 40,
    FileAlreadyOpen = 41,
    LogWriteFailed = 42,
    CursorNotFound = 43,
    UserDataInconsistent = 45,
    LockBusy = 46,
    NoMatchingDocument = 47,
    NamespaceExists = 48,
    InvalidRoleModification = 49,
    ExceededTimeLimit = 50,
    ManualInterventionRequired = 51,
    DollarPrefixedFieldName = 52,
    InvalidIdField = 53,
    NotSingleValueField = 54,
    InvalidDBRef = 55,
    EmptyFieldName = 56,
    DottedFieldName = 57,
    RoleModificationFailed = 58,
    CommandNotFound = 59,
    OBSOLETE_DatabaseNotFound = 60,
    ShardKeyNotFound = 61,
    OplogOperationUnsupported = 62,
    StaleShardVersion = 63,
    WriteConcernFailed = 64,
    MultipleErrorsOccurred = 65,
    ImmutableField = 66,
    CannotCreateIndex = 67 ,
    IndexAlreadyExists = 68 ,
    AuthSchemaIncompatible = 69,
    ShardNotFound = 70,
    ReplicaSetNotFound = 71,
    InvalidOptions = 72,
    InvalidNamespace = 73,
    NodeNotFound = 74,
    WriteConcernLegacyOK = 75,
    NoReplicationEnabled = 76,
    OperationIncomplete = 77,
    CommandResultSchemaViolation = 78,
    UnknownReplWriteConcern = 79,
    RoleDataInconsistent = 80,
    NoMatchParseContext = 81,
    NoProgressMade = 82,
    RemoteResultsUnavailable = 83,
    DuplicateKeyValue = 84,
    IndexOptionsConflict = 85 ,
    IndexKeySpecsConflict = 86 ,
    CannotSplit = 87,
    SplitFailed_OBSOLETE = 88,
    NetworkTimeout = 89,
    CallbackCanceled = 90,
    ShutdownInProgress = 91,
    SecondaryAheadOfPrimary = 92,
    InvalidReplicaSetConfig = 93,
    NotYetInitialized = 94,
    NotSecondary = 95,
    OperationFailed = 96,
    NoProjectionFound = 97,
    DBPathInUse = 98,
    CannotSatisfyWriteConcern = 100,
    OutdatedClient = 101,
    IncompatibleAuditMetadata = 102,
    NewReplicaSetConfigurationIncompatible = 103,
    NodeNotElectable = 104,
    IncompatibleShardingMetadata = 105,
    DistributedClockSkewed = 106,
    LockFailed = 107,
    InconsistentReplicaSetNames = 108,
    ConfigurationInProgress = 109,
    CannotInitializeNodeWithData = 110,
    NotExactValueField = 111,
    WriteConflict = 112,
    InitialSyncFailure = 113,
    InitialSyncOplogSourceMissing = 114,
    CommandNotSupported = 115,
    DocTooLargeForCapped = 116,
    ConflictingOperationInProgress = 117,
    NamespaceNotSharded = 118,
    InvalidSyncSource = 119,
    OplogStartMissing = 120,
    DocumentValidationFailure = 121,
    OBSOLETE_ReadAfterOptimeTimeout = 122,
    NotAReplicaSet = 123,
    IncompatibleElectionProtocol = 124,
    CommandFailed = 125,
    RPCProtocolNegotiationFailed = 126,
    UnrecoverableRollbackError = 127,
    LockNotFound = 128,
    LockStateChangeFailed = 129,
    SymbolNotFound = 130,
    RLPInitializationFailed = 131,
    OBSOLETE_ConfigServersInconsistent = 132,
    FailedToSatisfyReadPreference = 133,
    ReadConcernMajorityNotAvailableYet = 134,
    StaleTerm = 135,
    CappedPositionLost = 136,
    IncompatibleShardingConfigVersion = 137,
    RemoteOplogStale = 138,
    JSInterpreterFailure = 139,
    InvalidSSLConfiguration = 140,
    SSLHandshakeFailed = 141,
    JSUncatchableError = 142,
    CursorInUse = 143,
    IncompatibleCatalogManager = 144,
    PooledConnectionsDropped = 145,
    ExceededMemoryLimit = 146,
    ZLibError = 147,
    ReadConcernMajorityNotEnabled = 148,
    NoConfigMaster = 149,
    StaleEpoch = 150,
    OperationCannotBeBatched = 151,
    OplogOutOfOrder = 152,
    ChunkTooBig = 153,
    InconsistentShardIdentity = 154,
    CannotApplyOplogWhilePrimary = 155,
    NeedsDocumentMove = 156,
    CanRepairToDowngrade = 157,
    MustUpgrade = 158,
    DurationOverflow = 159,
    MaxStalenessOutOfRange = 160,
    IncompatibleCollationVersion = 161,
    CollectionIsEmpty = 162,
    ZoneStillInUse = 163,
    InitialSyncActive = 164,
    ViewDepthLimitExceeded = 165,
    CommandNotSupportedOnView = 166,
    OptionNotSupportedOnView = 167,
    InvalidPipelineOperator = 168,
    CommandOnShardedViewNotSupportedOnMongod = 169,
    TooManyMatchingDocuments = 170,
    CannotIndexParallelArrays = 171,
    TransportSessionClosed = 172,
    TransportSessionNotFound = 173,
    TransportSessionUnknown = 174,
    QueryPlanKilled = 175,
    FileOpenFailed = 176,
    ZoneNotFound = 177,
    RangeOverlapConflict = 178,
    WindowsPdhError = 179,
    BadPerfCounterPath = 180,
    AmbiguousIndexKeyPattern = 181,
    InvalidViewDefinition = 182,
    ClientMetadataMissingField = 183,
    ClientMetadataAppNameTooLarge = 184,
    ClientMetadataDocumentTooLarge = 185,
    ClientMetadataCannotBeMutated = 186,
    LinearizableReadConcernError = 187,
    IncompatibleServerVersion = 188,
    PrimarySteppedDown = 189,
    MasterSlaveConnectionFailure = 190,
    OBSOLETE_BalancerLostDistributedLock = 191,
    FailPointEnabled = 192,
    NoShardingEnabled = 193,
    BalancerInterrupted = 194,
    ViewPipelineMaxSizeExceeded = 195,
    InvalidIndexSpecificationOption = 197,
    OBSOLETE_ReceivedOpReplyMessage = 198,
    ReplicaSetMonitorRemoved = 199,
    ChunkRangeCleanupPending = 200,
    CannotBuildIndexKeys = 201,
    NetworkInterfaceExceededTimeLimit = 202,
    ShardingStateNotInitialized = 203,
    TimeProofMismatch = 204,
    ClusterTimeFailsRateLimiter = 205,
    NoSuchSession = 206,
    InvalidUUID = 207,
    TooManyLocks = 208,
    StaleClusterTime = 209,
    CannotVerifyAndSignLogicalTime = 210,
    KeyNotFound = 211,
    IncompatibleRollbackAlgorithm = 212,
    DuplicateSession = 213,
    AuthenticationRestrictionUnmet = 214,
    DatabaseDropPending = 215,
    ElectionInProgress = 216,
    IncompleteTransactionHistory = 217,
    UpdateOperationFailed = 218,
    FTDCPathNotSet = 219,
    FTDCPathAlreadySet = 220,
    IndexModified = 221,
    CloseChangeStream = 222,
    IllegalOpMsgFlag = 223,
    JSONSchemaNotAllowed = 224,
    TransactionTooOld = 225,

    SocketException = 9001,
    OBSOLETE_RecvStaleConfig = 9996,
    NotMaster = 10107,
    CannotGrowDocumentInCappedNamespace = 10003,
    DuplicateKey = 11000,
    InterruptedAtShutdown = 11600,
    Interrupted = 11601,
    InterruptedDueToReplStateChange = 11602,
    OutOfDiskSpace = 14031 ,
    KeyTooLong = 17280,
    BackgroundOperationInProgressForDatabase = 12586,
    BackgroundOperationInProgressForNamespace = 12587,
    NotMasterOrSecondary = 13436,
    NotMasterNoSlaveOk = 13435,
    ShardKeyTooBig = 13334,
    StaleConfig = 13388,
    DatabaseDifferCase = 13297,
    OBSOLETE_PrepareConfigsFailed = 13104);

type
  { Is raised when there is an error writing to the database }
  EgoMongoDBWriteError = class(EgoMongoDBError)
  {$REGION 'Internal Declarations'}
  private
    FErrorCode: TgoMongoErrorCode;
  {$ENDREGION 'Internal Declarations'}
  public
    constructor Create(const AErrorCode: TgoMongoErrorCode;
      const AErrorMsg: String);

    { The MongoDB error code }
    property ErrorCode: TgoMongoErrorCode read FErrorCode;
  end;

type
  { Forward declarations }
  IgoMongoDatabase = interface;
  IgoMongoCollection = interface;

  { The client interface to MongoDB.
    This is the entry point for the MongoDB API.
    This interface is implemented in to TgoMongoClient class. }
  IgoMongoClient = interface
  ['{66FF5346-48F6-44E1-A46F-D8B958F06EA0}']
    { Returns an array with the names of all databases available to the client. }
    function ListDatabaseNames: TArray<String>;

    { Returns an array of documents describing all databases available to the
      client (one document per database). The structure of each document is
      described here:
      https://docs.mongodb.com/manual/reference/command/listDatabases/ }
    function ListDatabases: TArray<TgoBsonDocument>;

    { Drops the database with the specified name.

      Parameters:
        AName: The name of the database to drop. }
    procedure DropDatabase(const AName: String);

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
    function GetDatabase(const AName: String): IgoMongoDatabase;
  end;

  { Represents a database in MongoDB.
    Instances of this interface are aquired by calling
    IgoMongoClient.GetDatabase. }
  IgoMongoDatabase = interface
  ['{5164D7B1-74F5-45F1-AE22-AB5FFC834590}']
    {$REGION 'Internal Declarations'}
    function _GetClient: IgoMongoClient;
    function _GetName: String;
    {$ENDREGION 'Internal Declarations'}

    { Returns an array with the names of all collections in the database. }
    function ListCollectionNames: TArray<String>;

    { Returns an array of documents describing all collections in the database
      (one document per collection). The structure of each document is
      described here:
      https://docs.mongodb.com/manual/reference/method/db.getCollectionInfos/ }
    function ListCollections: TArray<TgoBsonDocument>;

    { Drops the collection with the specified name.

      Parameters:
        AName: The name of the collection to drop. }
    procedure DropCollection(const AName: String);

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
    function GetCollection(const AName: String): IgoMongoCollection;

    { The client used for this database. }
    property Client: IgoMongoClient read _GetClient;

    { The name of the database. }
    property Name: String read _GetName;
  end;

  { Represents a cursor to the documents returned from one of the
    IgoMongoCollection.Find methods. }
  IgoMongoCursor = interface
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
    function _GetName: String;
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
    function InsertMany(const ADocuments: array of TgoBsonDocument;
      const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TArray<TgoBsonDocument>;
      const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TEnumerable<TgoBsonDocument>;
      const AOrdered: Boolean = True): Integer; overload;

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
    function DeleteMany(const AFilter: TgoMongoFilter;
      const AOrdered: Boolean = True): Integer;

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
    function UpdateOne(const AFilter: TgoMongoFilter;
      const AUpdate: TgoMongoUpdate; const AUpsert: Boolean = False): Boolean;

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
    function UpdateMany(const AFilter: TgoMongoFilter;
      const AUpdate: TgoMongoUpdate; const AUpsert: Boolean = False;
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
    function Find(const AFilter: TgoMongoFilter;
      const AProjection: TgoMongoProjection): IgoMongoCursor; overload;
    function Find(const AFilter: TgoMongoFilter): IgoMongoCursor; overload;
    function Find(const AProjection: TgoMongoProjection): IgoMongoCursor; overload;
    function Find: IgoMongoCursor; overload;
    function Find(const AFilter: TgoMongoFilter;
      const ASort: TgoMongoSort): IgoMongoCursor; overload;
    function Find(const AFilter: TgoMongoFilter;
      const AProjection: TgoMongoProjection;
      const ASort: TgoMongoSort): IgoMongoCursor; overload;

    { Finds the first document matching the filter.

      Parameters:
        AFilter: filter containing query operators to search for the document
          that matches the filter.
        AProjection: (optional) projection that specifies the fields to return
          in the document that matches the query filter. If not specified, then
          all fields are returned.

      Returns:
        The first document that matches the filter. If no documents match the
        filter, then a null-documents is returned (call its IsNil method to
        check for this). }
    function FindOne(const AFilter: TgoMongoFilter;
      const AProjection: TgoMongoProjection): TgoBsonDocument; overload;
    function FindOne(const AFilter: TgoMongoFilter): TgoBsonDocument; overload;

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

      Returns:
        Created or not. }
    function CreateIndex(const AName : String; const AKeyFields : Array of String; const AUnique : Boolean = false): Boolean; overload;

    { The database that contains this collection. }
    property Database: IgoMongoDatabase read _GetDatabase;

    { The name of the collection. }
    property Name: String read _GetName;
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
    PrivateKeyPassword: String;

    { Authentication mechanism }
    AuthMechanism: TgoMongoAuthMechanism;

    { Authentication database }
    AuthDatabase: String;

    { Authentication username }
    Username: String;

    { Authentication password }
    Password: String;
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
  protected
    { IgoMongoClient }
    function ListDatabaseNames: TArray<String>;
    function ListDatabases: TArray<TgoBsonDocument>;
    procedure DropDatabase(const AName: String);
    function GetDatabase(const AName: String): IgoMongoDatabase;
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
    constructor Create(const AHost: String = DEFAULT_HOST;
      const APort: Integer = DEFAULT_PORT); overload;
    constructor Create(const AHost: String; const APort: Integer;
      const ASettings: TgoMongoClientSettings); overload;
    constructor Create(const ASettings: TgoMongoClientSettings); overload;
    destructor Destroy; override;
  end;

resourcestring
  RS_MONGODB_CONNECTION_ERROR = 'Error connecting to the MongoDB database';
  RS_MONGODB_GENERIC_ERROR = 'Unspecified error while performing MongoDB operation';

implementation

uses
  System.Math;

{$POINTERMATH ON}

const
  { Virtual collection that is used for query commands }
  COLLECTION_COMMAND = '$cmd';

  { System collections }
  COLLECTION_ADMIN = 'admin';
  COLLECTION_ADMIN_COMMAND = COLLECTION_ADMIN + '.' + COLLECTION_COMMAND;

  { Maximum number of documents that can be written in bulk at once }
  MAX_BULK_SIZE      = 1000;

procedure HandleTimeout(const AReply: IgoMongoReply); inline;
begin
  if (AReply = nil) then
    raise EgoMongoDBConnectionError.Create(RS_MONGODB_CONNECTION_ERROR);
end;

function HandleCommandReply(const AReply: IgoMongoReply;
  const AErrorToIgnore: TgoMongoErrorCode = TgoMongoErrorCode.OK): Integer;
var
  Doc, ErrorDoc: TgoBsonDocument;
  Value: TgoBsonValue;
  Values: TgoBsonArray;
  Ok: Boolean;
  ErrorCode: TgoMongoErrorCode;
  ErrorMsg: String;
begin
  if (AReply = nil) then
    raise EgoMongoDBConnectionError.Create(RS_MONGODB_CONNECTION_ERROR);

  if (AReply.Documents = nil) then
    { Everything OK }
    Exit(0);

  Doc := TgoBsonDocument.Load(AReply.Documents[0]);
  { Return number of documents affected }
  Result := Doc['n'];

  Ok := Doc['ok'];
  if (not Ok) then
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
    FName: String;
    FFullCommandCollectionName: UTF8String;
  protected
    { IgoMongoDatabase }
    function _GetClient: IgoMongoClient;
    function _GetName: String;

    function ListCollectionNames: TArray<String>;
    function ListCollections: TArray<TgoBsonDocument>;
    procedure DropCollection(const AName: String);
    function GetCollection(const AName: String): IgoMongoCollection;
  protected
    property Protocol: TgoMongoProtocol read FProtocol;
    property Name: String read FName;
    property FullCommandCollectionName: UTF8String read FFullCommandCollectionName;
  {$ENDREGION 'Internal Declarations'}
  public
    constructor Create(const AClient: TgoMongoClient; const AName: String);
  end;

type
  { Implements IgoMongoCursor }
  TgoMongoCursor = class(TInterfacedObject, IgoMongoCursor)
  {$REGION 'Internal Declarations'}
  private type
    TEnumerator = class(TEnumerator<TgoBsonDocument>)
    private
      FProtocol: TgoMongoProtocol; // Reference
      FFullCollectionName: UTF8String;
      FPage: TArray<TBytes>;
      FCursorId: Int64;
      FIndex: Integer;
    private
      procedure GetMore;
    protected
      function DoGetCurrent: TgoBsonDocument; override;
      function DoMoveNext: Boolean; override;
    public
      constructor Create(const AProtocol: TgoMongoProtocol;
        const AFullCollectionName: UTF8String; const APage: TArray<TBytes>;
        const ACursorId: Int64);
    end;
  private
    FProtocol: TgoMongoProtocol; // Reference
    FFullCollectionName: UTF8String;
    FInitialPage: TArray<TBytes>;
    FInitialCursorId: Int64;
  public
    { IgoMongoCursor }
    function GetEnumerator: TEnumerator<TgoBsonDocument>;
    function ToArray: TArray<TgoBsonDocument>;
  public
    constructor Create(const AProtocol: TgoMongoProtocol;
      const AFullCollectionName: UTF8String; const AInitialPage: TArray<TBytes>;
      const AInitialCursorId: Int64);
  {$ENDREGION 'Internal Declarations'}
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
    FName: String;
    FFullName: UTF8String;
    FFullCommandCollectionName: UTF8String;
  private
    procedure AddWriteConcern(const AWriter: IgoBsonWriter);
    function InsertMany(const ADocuments: PgoBsonDocument;
      const ACount: Integer; const AOrdered: Boolean): Integer; overload;
    function Delete(const AFilter: TgoMongoFilter; const AOrdered: Boolean;
      const ALimit: Integer): Integer;
    function Update(const AFilter: TgoMongoFilter;
      const AUpdate: TgoMongoUpdate; const AUpsert, AOrdered,
      AMulti: Boolean): Integer;
    function Find(const AFilter, AProjection: TBytes): IgoMongoCursor; overload;
    function FindOne(const AFilter, AProjection: TBytes): TgoBsonDocument; overload;
  private
    class function AddModifier(const AFilter: TgoMongoFilter;
      const ASort: TgoMongoSort): TBytes; static;
  protected
    { IgoMongoCollection }
    function _GetDatabase: IgoMongoDatabase;
    function _GetName: String;

    function InsertOne(const ADocument: TgoBsonDocument): Boolean;
    function InsertMany(const ADocuments: array of TgoBsonDocument;
      const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TArray<TgoBsonDocument>;
      const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TEnumerable<TgoBsonDocument>;
      const AOrdered: Boolean = True): Integer; overload;

    function DeleteOne(const AFilter: TgoMongoFilter): Boolean;
    function DeleteMany(const AFilter: TgoMongoFilter;
      const AOrdered: Boolean = True): Integer;

    function UpdateOne(const AFilter: TgoMongoFilter;
      const AUpdate: TgoMongoUpdate; const AUpsert: Boolean = False): Boolean;
    function UpdateMany(const AFilter: TgoMongoFilter;
      const AUpdate: TgoMongoUpdate; const AUpsert: Boolean = False;
      const AOrdered: Boolean = True): Integer;

    function Find(const AFilter: TgoMongoFilter;
      const AProjection: TgoMongoProjection): IgoMongoCursor; overload;
    function Find(const AFilter: TgoMongoFilter): IgoMongoCursor; overload;
    function Find(const AProjection: TgoMongoProjection): IgoMongoCursor; overload;
    function Find: IgoMongoCursor; overload;
    function Find(const AFilter: TgoMongoFilter;
      const ASort: TgoMongoSort): IgoMongoCursor; overload;
    function Find(const AFilter: TgoMongoFilter;
      const AProjection: TgoMongoProjection;
      const ASort: TgoMongoSort): IgoMongoCursor; overload;
    function FindOne(const AFilter: TgoMongoFilter;
      const AProjection: TgoMongoProjection): TgoBsonDocument; overload;
    function FindOne(const AFilter: TgoMongoFilter): TgoBsonDocument; overload;

    function Count: Integer; overload;
    function Count(const AFilter: TgoMongoFilter): Integer; overload;

    function CreateIndex(const AName : String; const AKeyFields : Array of String; const AUnique : Boolean = false): Boolean;
  {$ENDREGION 'Internal Declarations'}
  public
    constructor Create(const ADatabase: TgoMongoDatabase; const AName: String);
  end;

{ EgoMongoDBWriteError }

constructor EgoMongoDBWriteError.Create(const AErrorCode: TgoMongoErrorCode;
  const AErrorMsg: String);
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
  Result.Secure := False;
  Result.Certificate := nil;
  Result.PrivateKey := nil;
  Result.PrivateKeyPassword := '';
  Result.AuthMechanism := TgoMongoAuthMechanism.None;
  Result.AuthDatabase := '';
  Result.Username := '';
  Result.Password := '';
end;

{ TgoMongoClient }

constructor TgoMongoClient.Create(const AHost: String; const APort: Integer);
begin
  Create(AHost, APort, TgoMongoClientSettings.Create);
end;

constructor TgoMongoClient.Create(const AHost: String; const APort: Integer;
  const ASettings: TgoMongoClientSettings);
var
  S: TgoMongoProtocolSettings;
begin
  inherited Create;
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

procedure TgoMongoClient.DropDatabase(const AName: String);
// https://docs.mongodb.com/manual/reference/command/dropDatabase/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('dropDatabase', 1);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpQuery(UTF8String(AName + '.' + COLLECTION_COMMAND),
    [], 0, -1, Writer.ToBson, nil);
  HandleCommandReply(Reply);
end;

function TgoMongoClient.GetDatabase(const AName: String): IgoMongoDatabase;
begin
  Result := TgoMongoDatabase.Create(Self, AName);
end;

function TgoMongoClient.ListDatabaseNames: TArray<String>;
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
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('listDatabases', 1);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpQuery(COLLECTION_ADMIN_COMMAND, [], 0, -1, Writer.ToBson, nil);
  HandleCommandReply(Reply);
  if (Reply.Documents = nil) then
    Exit(nil);

  Doc := TgoBsonDocument.Load(Reply.Documents[0]);
  if (not Doc.TryGetValue('databases', Value)) then
    Exit(nil);
  Databases := Value.AsBsonArray;

  SetLength(Result, Databases.Count);
  for I := 0 to Databases.Count - 1 do
    Result[I] := Databases[I].AsBsonDocument;
end;

{ TgoMongoDatabase }

constructor TgoMongoDatabase.Create(const AClient: TgoMongoClient;
  const AName: String);
begin
  Assert(AClient <> nil);
  Assert(AName <> '');
  inherited Create;
  FClient := AClient;
  FName := AName;
  FFullCommandCollectionName := UTF8String(AName + '.' + COLLECTION_COMMAND);
  FProtocol := AClient.Protocol;
  Assert(FProtocol <> nil);
end;

procedure TgoMongoDatabase.DropCollection(const AName: String);
// https://docs.mongodb.com/manual/reference/command/drop/#dbcmd.drop
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('drop', AName);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
  HandleCommandReply(Reply, TgoMongoErrorCode.NamespaceNotFound);
end;

function TgoMongoDatabase.GetCollection(
  const AName: String): IgoMongoCollection;
begin
  Result := TgoMongoCollection.Create(Self, AName);
end;

function TgoMongoDatabase.ListCollectionNames: TArray<String>;
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
  Doc, Cursor: TgoBsonDocument;
  Value: TgoBsonValue;
  Docs: TgoBsonArray;
  I: Integer;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('listCollections', 1);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
  HandleCommandReply(Reply);
  if (Reply.Documents = nil) then
    Exit(nil);

  { NOTE: The reply contains a cursor with the first batch of documents.
    However, the batch always seems to contain the documents for ALL collections
    collections in the database, so there is no need for manual paging. }
  Doc := TgoBsonDocument.Load(Reply.Documents[0]);
  if (not Doc.TryGetValue('cursor', Value)) then
    Exit(nil);
  Cursor := Value.AsBsonDocument;

  if (not Cursor.TryGetValue('firstBatch', Value)) then
    Exit(nil);

  Docs := Value.AsBsonArray;
  SetLength(Result, Docs.Count);
  for I := 0 to Docs.Count - 1 do
    Result[I] := Docs[I].AsBsonDocument;
end;

function TgoMongoDatabase._GetClient: IgoMongoClient;
begin
  Result := FClient;
end;

function TgoMongoDatabase._GetName: String;
begin
  Result := FName;
end;

{ TgoMongoCursor }

constructor TgoMongoCursor.Create(const AProtocol: TgoMongoProtocol;
  const AFullCollectionName: UTF8String; const AInitialPage: TArray<TBytes>;
  const AInitialCursorId: Int64);
begin
  inherited Create;
  FProtocol := AProtocol;
  FFullCollectionName := AFullCollectionName;
  FInitialPage := AInitialPage;
  FInitialCursorId := AInitialCursorId;
end;

function TgoMongoCursor.GetEnumerator: TEnumerator<TgoBsonDocument>;
begin
  Result := TEnumerator.Create(FProtocol, FFullCollectionName, FInitialPage,
    FInitialCursorId);
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

constructor TgoMongoCursor.TEnumerator.Create(const AProtocol: TgoMongoProtocol;
  const AFullCollectionName: UTF8String; const APage: TArray<TBytes>;
  const ACursorId: Int64);
begin
  inherited Create;
  FProtocol := AProtocol;
  FFullCollectionName := AFullCollectionName;
  FPage := APage;
  FCursorId := ACursorId;
  FIndex := -1;
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
begin
  { NOTE: We could pass 0 for the ANumberToReturn parameter, but that seems to
          return all remaining documents, instead of the next page.
          So instead we use the current page size. }
  Reply := FProtocol.OpGetMore(FFullCollectionName, Length(FPage), FCursorId);
  HandleTimeout(Reply);
  FPage := Reply.Documents;
  FCursorId := Reply.CursorId;
  FIndex := 0;
end;

{ TgoMongoCollection }

class function TgoMongoCollection.AddModifier(const AFilter: TgoMongoFilter;
  const ASort: TgoMongoSort): TBytes;
var
  Writer: IgoBsonWriter;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteName('$query');
  Writer.WriteRawBsonDocument(AFilter.ToBson);
  Writer.WriteName('$orderby');
  Writer.WriteRawBsonDocument(ASort.ToBson);
  Writer.WriteEndDocument;
  Result := Writer.ToBson;
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
  Writer.WriteName('query');
  Writer.WriteRawBsonDocument(AFilter.ToBson);
  Writer.WriteEndDocument;

  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
  Result := HandleCommandReply(Reply);
end;

constructor TgoMongoCollection.Create(const ADatabase: TgoMongoDatabase;
  const AName: String);
begin
  Assert(Assigned(ADatabase));
  Assert(AName <> '');
  inherited Create;
  FDatabase := ADatabase;
  FName := AName;
  FFullName := UTF8String(ADatabase.Name + '.' + AName);
  FFullCommandCollectionName := ADatabase.FullCommandCollectionName;
  FProtocol := ADatabase.Protocol;
  Assert(FProtocol <> nil);
end;

function TgoMongoCollection.CreateIndex(const AName : String;
  const AKeyFields : Array of String; const AUnique : Boolean = false): Boolean;
// https://docs.mongodb.com/manual/reference/command/createIndexes/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  i: Integer;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('createIndexes', FName);

  Writer.WriteStartArray('indexes');
  Writer.WriteStartDocument;
  Writer.WriteStartDocument('key');
  for i:=0 to High(AKeyFields) do
    Writer.WriteInt32(AKeyFields[i],1);
  Writer.WriteEndDocument;
  Writer.WriteString('name', AName);
  Writer.WriteBoolean('unique', AUnique);
  Writer.WriteEndDocument;
  Writer.WriteEndArray;

  AddWriteConcern(Writer);

  Writer.WriteEndDocument;

  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
  Result := (HandleCommandReply(Reply) = 1);
end;

function TgoMongoCollection.Delete(const AFilter: TgoMongoFilter;
  const AOrdered: Boolean; const ALimit: Integer): Integer;
// https://docs.mongodb.com/manual/reference/command/delete/
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;

  Writer.WriteString('delete', FName);

  Writer.WriteStartArray('deletes');
  Writer.WriteStartDocument;
  Writer.WriteName('q');
  Writer.WriteRawBsonDocument(AFilter.ToBson);
  Writer.WriteInt32('limit', ALimit);
  Writer.WriteEndDocument;
  Writer.WriteEndArray;

  AddWriteConcern(Writer);
  Writer.WriteEndDocument;

  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
  Result := HandleCommandReply(Reply);
end;

function TgoMongoCollection.DeleteMany(const AFilter: TgoMongoFilter;
  const AOrdered: Boolean): Integer;
begin
  Result := Delete(AFilter, AOrdered, 0);
end;

function TgoMongoCollection.DeleteOne(const AFilter: TgoMongoFilter): Boolean;
begin
  Result := (Delete(AFilter, True, 1) = 1);
end;

function TgoMongoCollection.Find(const AFilter: TgoMongoFilter): IgoMongoCursor;
begin
  Result := Find(AFilter.ToBson, nil);
end;

function TgoMongoCollection.Find(const AFilter: TgoMongoFilter;
  const AProjection: TgoMongoProjection): IgoMongoCursor;
begin
  Result := Find(AFilter.ToBson, AProjection.ToBson);
end;

function TgoMongoCollection.Find: IgoMongoCursor;
begin
  Result := Find(nil, nil);
end;

function TgoMongoCollection.Find(
  const AProjection: TgoMongoProjection): IgoMongoCursor;
begin
  Result := Find(nil, AProjection.ToBson);
end;

function TgoMongoCollection.Find(const AFilter: TgoMongoFilter;
  const ASort: TgoMongoSort): IgoMongoCursor;
begin
  Result := Find(AddModifier(AFilter, ASort), nil);
end;

function TgoMongoCollection.Find(const AFilter: TgoMongoFilter;
  const AProjection: TgoMongoProjection;
  const ASort: TgoMongoSort): IgoMongoCursor;
begin
  Result := Find(AddModifier(AFilter, ASort), AProjection.ToBson);
end;

function TgoMongoCollection.Find(const AFilter,
  AProjection: TBytes): IgoMongoCursor;
// https://docs.mongodb.com/manual/reference/method/db.collection.find
var
  Reply: IgoMongoReply;
begin
  Reply := FProtocol.OpQuery(FFullName, [], 0, 0, AFilter, AProjection);
  HandleTimeout(Reply);
  Result := TgoMongoCursor.Create(FProtocol, FFullName, Reply.Documents, Reply.CursorId);
end;

function TgoMongoCollection.FindOne(const AFilter: TgoMongoFilter;
  const AProjection: TgoMongoProjection): TgoBsonDocument;
begin
  Result := FindOne(AFilter.ToBson, AProjection.ToBson);
end;

function TgoMongoCollection.FindOne(
  const AFilter: TgoMongoFilter): TgoBsonDocument;
begin
  Result := FindOne(AFilter.ToBson, nil);
end;

function TgoMongoCollection.FindOne(const AFilter,
  AProjection: TBytes): TgoBsonDocument;
// https://docs.mongodb.com/manual/reference/method/db.collection.find
var
  Reply: IgoMongoReply;
begin
  Reply := FProtocol.OpQuery(FFullName, [], 0, 1, AFilter, AProjection);
  HandleTimeout(Reply);
  if (Reply.Documents = nil) then
    Result.SetNil
  else
    Result := TgoBsonDocument.Load(Reply.Documents[0]);
end;

function TgoMongoCollection.InsertMany(
  const ADocuments: array of TgoBsonDocument; const AOrdered: Boolean): Integer;
begin
  if (Length(ADocuments) > 0) then
    Result := InsertMany(@ADocuments[0], Length(ADocuments), AOrdered)
  else
    Result := 0;
end;

function TgoMongoCollection.InsertMany(
  const ADocuments: TArray<TgoBsonDocument>; const AOrdered: Boolean): Integer;
begin
  if (Length(ADocuments) > 0) then
    Result := InsertMany(@ADocuments[0], Length(ADocuments), AOrdered)
  else
    Result := 0;
end;

function TgoMongoCollection.InsertMany(const ADocuments: PgoBsonDocument;
  const ACount: Integer; const AOrdered: Boolean): Integer;
// https://docs.mongodb.com/manual/reference/command/insert/#dbcmd.insert
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  I, Remaining, ItemsInBatch, Index: Integer;
begin
  Remaining := ACount;
  Index := 0;
  Result := 0;
  while (Remaining > 0) do
  begin
    Writer := TgoBsonWriter.Create;
    Writer.WriteStartDocument;
    Writer.WriteString('insert', FName);

    Writer.WriteStartArray('documents');
    ItemsInBatch := Min(Remaining, MAX_BULK_SIZE);
    for I := 0 to ItemsInBatch - 1 do
    begin
      Writer.WriteValue(ADocuments[Index]);
      Inc(Index);
    end;
    Dec(Remaining, ItemsInBatch);
    Writer.WriteEndArray;

    Writer.WriteBoolean('ordered', AOrdered);

    AddWriteConcern(Writer);

    Writer.WriteEndDocument;

    Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
    Inc(Result, HandleCommandReply(Reply));
  end;
  Assert(Index = ACount);
end;

function TgoMongoCollection.InsertMany(
  const ADocuments: TEnumerable<TgoBsonDocument>; const AOrdered: Boolean): Integer;
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

  Writer.WriteStartArray('documents');
  Writer.WriteValue(ADocument);
  Writer.WriteEndArray;

  AddWriteConcern(Writer);

  Writer.WriteEndDocument;

  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
  Result := (HandleCommandReply(Reply) = 1);
end;

function TgoMongoCollection.Update(const AFilter: TgoMongoFilter;
  const AUpdate: TgoMongoUpdate; const AUpsert, AOrdered,
  AMulti: Boolean): Integer;
// https://docs.mongodb.com/manual/reference/command/update
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('update', FName);

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

  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
  Result := HandleCommandReply(Reply);
end;

function TgoMongoCollection.UpdateMany(const AFilter: TgoMongoFilter;
  const AUpdate: TgoMongoUpdate; const AUpsert, AOrdered: Boolean): Integer;
begin
  Result := Update(AFilter, AUpdate, AUpsert, AOrdered, True);
end;

function TgoMongoCollection.UpdateOne(const AFilter: TgoMongoFilter;
  const AUpdate: TgoMongoUpdate; const AUpsert: Boolean): Boolean;
begin
  Result := (Update(AFilter, AUpdate, AUpsert, False, False) = 1);
end;

function TgoMongoCollection._GetDatabase: IgoMongoDatabase;
begin
  Result := FDatabase;
end;

function TgoMongoCollection._GetName: String;
begin
  Result := FName;
end;

end.
