unit Grijjy.MongoDB.Protocol;
{ < Implements the MongoDB Wire Protocol.
  This unit is only used internally. }

{$INCLUDE 'Grijjy.inc'}

interface

uses
  System.SyncObjs,
  System.SysUtils,
  System.Generics.Collections,
  Grijjy.SysUtils,
{$IF Defined(MSWINDOWS)}
  Grijjy.SocketPool.Win,
{$ELSEIF Defined(LINUX)}
  Grijjy.SocketPool.Linux,
{$ELSE}
{$MESSAGE Error 'The MongoDB driver is only supported on Windows and Linux'}
{$ENDIF}
  Grijjy.Bson;

const
  { Virtual collection that is used for query commands }
  COLLECTION_COMMAND = '$cmd';
  { System collections }
  DB_ADMIN = 'admin';

type
  { Base class for MongoDB errors }
  EgoMongoDBError = class(Exception);

  { Is raised when a connection error (or timeout) occurs. }
  EgoMongoDBConnectionError = class(EgoMongoDBError);

  { Query flags as used by TgoMongoProtocol.OpQuery }
  TgoMongoQueryFlag = (
    { Tailable means cursor is not closed when the last data is retrieved.
      Rather, the cursor marks the final object’s position.
      You can resume using the cursor later, from where it was located,
      if more data were received. Like any “latent cursor”, the cursor may
      become invalid at some point (CursorNotFound) – for example if the final
      object it references were deleted. }
    TailableCursor = 1, // =>DBQuery.Option.Tailable   bit 1 = value 2

    { Allow query of replica slave. Normally these return an error except for
      namespace “local”. }
    SlaveOk = 2, // =>DBQuery.Option.SlaveOK bit 2 = value 4

    { Internal replication use only - driver should not set. }
    OplogRelay = 3,

    { The server normally times out idle cursors after an inactivity period
      (10 minutes) to prevent excess memory use. Set this option to prevent
      that. }
    NoCursorTimeout = 4, // =>DBQuery.Option.NoTimeout   bit 4 = 16

    { Use with TailableCursor. If we are at the end of the data, block for a
      while rather than returning no data. After a timeout period, we do return
      as normal. }
    AwaitData = 5, // =>DBQuery.Option.AwaitData   bit 5=32

    { Stream the data down full blast in multiple “more” packages, on the
      assumption that the client will fully read all data queried. Faster when
      you are pulling a lot of data and know you want to pull it all down.
      Note: the client is not allowed to not read all the data unless it closes the connection. }
    Exhaust = 6, // =>DBQuery.Option.Exhaust  bit 6=64

    { Get partial results from a mongos if some shards are down (instead of
      throwing an error) }
    Partial = 7); // =>DBQuery.Option.Partial  bit 7=128

  TgoMongoQueryFlags = set of TgoMongoQueryFlag;

  { Flags for new OP_MSG protocol }

  TGoMongoMsgFlag = (msgfChecksumPresent, msgfMoreToCome, msgfExhaustAllowed = 16);
  TgoMongoMsgFlags = set of TGoMongoMsgFlag;

type
  { Possible reponse flags as returned in IgoMongoReply.ReponseFlags. }
  TgoMongoResponseFlag = (
    { Is set when GetMore is called but the cursor id is not valid at the
      server. Returned with zero results. }
    CursorNotFound = 0,

    { Is set when query failed. Results consist of one document containing an
      “$err” field describing the failure. }
    QueryFailure = 1,

    { Drivers should ignore this. Only mongos will ever see this set, in which
      case, it needs to update config from the server. }
    ShardConfigStale = 2,

    { Is set when the server supports the AwaitData Query option. If it doesn’t,
      a client should sleep a little between getMore’s of a Tailable cursor.
      Mongod version 1.6 supports AwaitData and thus always sets AwaitCapable. }
    AwaitCapable = 3);
  TgoMongoResponseFlags = set of TgoMongoResponseFlag;

type
  { A reply to a query (see TgoMongoProtocol.OpQuery)..
    The OP_REPLY message is sent by the database in response
    to an OP_QUERY or OP_GET_MORE message. }

  IgoMongoReply = interface
    ['{25CEF8E1-B023-4232-BE9A-1FBE9E51CE57}']
{$REGION 'Internal Declarations'}
    function _GetResponseFlags: TgoMongoResponseFlags;
    function _GetCursorId: Int64;
    function _GetStartingFrom: Integer;
    function _GetResponseTo: Integer;
    function _GetDocuments: TArray<TBytes>;
    function _FirstDoc: TgoBsonDocument;
    function _GetDocumentNames: TArray<String>;
    function _GetDocumentTypes: TArray<Byte>;
{$ENDREGION 'Internal Declarations'}
    { Various reponse flags }
    property ReponseFlags: TgoMongoResponseFlags read _GetResponseFlags;

    { The cursorID that this reply is a part of. In the event that the result
      set of the query fits into one reply message, cursorID will be 0.
      This cursorID must be used in any GetMore messages used to get more data. }
    property CursorId: Int64 read _GetCursorId;

    { Starting position in the cursor. }
    property StartingFrom: Integer read _GetStartingFrom;

    { The identifier of the message that this reply is response to. }
    property ResponseTo: Integer read _GetResponseTo;

    { Raw BSON documents in the reply. }
    {This interface is obsolete}
    property FirstDoc: TgoBsonDocument read _FirstDoc;
    property Documents: TArray<TBytes> read _GetDocuments;
    Property DocumentNames:   TArray<String> read _GetDocumentNames;
    Property DocumentTypes:    TArray<Byte> read _GetDocumentTypes;
  end;

type
  { Mongo authentication mechanism }
  TgoMongoAuthMechanism = (None, SCRAM_SHA_1, SCRAM_SHA_256);

  { Customizable protocol settings. }
  TgoMongoProtocolSettings = record
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
  end;

type
  { Payload type 1 for OP_MSG }
  tgoPayloadType1 = record
    Name: string;
    Docs: TArray<TBytes>;
    procedure WriteTo(buffer: tgoByteBuffer);
  end;

  TgoMongoProtocol = class
{$REGION 'Internal Declarations'}
  private const
    OP_MSG = 2013;
    OP_REPLY = 1;
    OP_QUERY = 2004;
    OP_GET_MORE = 2005;
    OP_KILL_CURSORS = 2007;
    RECV_BUFFER_SIZE = 32768;
    EMPTY_DOCUMENT: array [0 .. 4] of Byte = (5, 0, 0, 0, 0);
  private
    class var FClientSocketManager: TgoClientSocketManager;
  private
    FHost: string;
    FPort: Integer;
    FSettings: TgoMongoProtocolSettings;
    FNextRequestId: Integer;
    FConnection: TgoSocketConnection;
    FConnectionLock: TCriticalSection;
    FCompletedReplies: TDictionary<Integer, IgoMongoReply>;
    FPartialReplies: TDictionary<Integer, TDateTime>;
    FRepliesLock: TCriticalSection;
    FRecvBuffer: TBytes;
    FRecvSize: Integer;
    FRecvBufferLock: TCriticalSection;
    FAuthErrorMessage: string;
    FAuthErrorCode: Integer;
    FMinWireVersion: Integer;
    FMaxWireVersion: Integer;
  private
    procedure Send(const AData: TBytes);
    procedure Recover;
    function WaitForReply(const ARequestId: Integer): IgoMongoReply;
    function TryGetReply(const ARequestId: Integer; out AReply: IgoMongoReply): Boolean; inline;
    function LastPartialReply(const ARequestId: Integer; out ALastRecv: TDateTime): Boolean;
    function OpReplyValid(out NewFormat: Boolean; out AIndex: Integer): Boolean;
    function HaveReplyMsgHeader(out AMsgHeader; tb: TBytes; Size: Integer): Boolean; overload;
    function HaveReplyMsgHeader(out AMsgHeader): Boolean; overload;
  private
    { Authentication }
    function saslStart(const APayload: string): IgoMongoReply;
    function saslContinue(const AConversationId: Integer; const APayload: string): IgoMongoReply;
    function Authenticate: Boolean;

    { Connection }
    function Connect: Boolean;
    function IsConnected: Boolean;
    function ConnectionState: TgoConnectionState; inline;
  private
    { Socket events }
    procedure SocketConnected;
    procedure SocketDisconnected;
    procedure SocketRecv(const ABuffer: Pointer; const ASize: Integer);
    procedure InitialHandshake;
    procedure RemoveReply(const ARequestId: Integer);
  public
    class constructor Create;
    class destructor Destroy;
{$ENDREGION 'Internal Declarations'}
  public
    { Creates the protocol.

      Parameters:
      AHost: host address of the MongoDB server to connect to.
      APort: connection port.
      ASettings: custom protocol settings. }
    constructor Create(const AHost: string; const APort: Integer; const ASettings: TgoMongoProtocolSettings);
    destructor Destroy; override;

    { Implements the OP_QUERY opcode, used to query the database for documents
      in a collection.

      Parameters:
      AFullCollectionName: the fully qualified name of the collection in the
      database to query (in <DatabaseName>.<CollectionName> form).
      AFlags: various query flags.
      ANumberToSkip: the number of documents to omit - starting from the first
      document in the resulting dataset - when returning the result of the
      query.
      ANumberToReturn: limits the number of documents in the first reply to
      the query. However, the database will still establish a cursor and
      return the cursorID to the client if there are more results than
      ANumberToReturn.
      If ANumberToReturn is 0, the db will use the default return size.
      If the number is negative, then the database will return that number
      and close the cursor. No further results for that query can be
      fetched.
      If ANumberToReturn is 1 the server will treat it as -1 (closing the
      cursor automatically).
      AQuery: raw BSON data with the document that represents the query.
      The query will contain one or more elements, all of which must match
      for a document to be included in the result set. Possible elements
      include $query, $orderby, $hint, $explain, and $snapshot.
      AReturnFieldsSelector: (optional) raw BSON data with a document that
      limits the fields in the returned documents. The AReturnFieldsSelector
      contains one or more elements, each of which is the name of a field
      that should be returned, and and the integer value 1.

      Returns:
      The reply to the query, or nil if the request timed out. }
    function OpQuery(const ADatabase, ACollectionName: string; const AFlags: TgoMongoQueryFlags;
      const ANumberToSkip, ANumberToReturn: Integer; const AQuery: TBytes; const AReturnFieldsSelector: TBytes = nil): IgoMongoReply;

    { Implements the OP_GET_MORE opcode, used to get an additional page of
      documents from the database.

      Parameters:
      AFullCollectionName: the fully qualified name of the collection in the
      database to query (in <DatabaseName>.<CollectionName> form).
      ANumberToReturn: limits the number of documents in the first reply to
      the query. However, the database will still establish a cursor and
      return the cursorID to the client if there are more results than
      ANumberToReturn.
      If ANumberToReturn is 0, the db will use the default return size.
      ACursorId: cursor identifier as returned in the reply from a previous
      call to OpQuery or OpGetMore.

      Returns:
      The reply to the query, or nil if the request timed out. }

    function OpGetMore(const ADatabaseName, ACollectionName: string; const ANumberToReturn: Integer; const ACursorId: Int64): IgoMongoReply;

    { Implements the OP_KILL_CURSORS opcode, used to free open cursors on the server.
      (wire protocol 3.0)

      Use case:
      Called by the destructor of tgoMongoCursor.tEnumerator.
      If the enumeration loop was exited prematurely without enumerating all elements,
      that would sometimes result in a resource leak on the server (orphaned cursor).

      Parameters:
      ACursorIds
      An array with the cursor ID's to release. The values should not be 0.

      See also:
      https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-kill_cursors }

    procedure OpKillCursors(const ADatabaseName, ACollectionName: string; const ACursorIds: TArray<Int64>);

    function OpMsg(const ParamType0: TBytes; const ParamsType1: TArray<tgoPayloadType1>; NoResponse: Boolean = False)
      : IgoMongoReply; overload;

    function SupportsOpMsg: Boolean;

  public
    { Authenticate error message if failed }
    property AuthErrorMessage: string read FAuthErrorMessage;

    { Authenticate error code if failed }
    property AuthErrorCode: Integer read FAuthErrorCode;
    property MinWireVersion: Integer read FMinWireVersion;
    property MaxWireVersion: Integer read FMaxWireVersion;
  end;

resourcestring
  RS_MONGODB_AUTHENTICATION_ERROR = 'Error authenticating [%d] %s';

implementation

uses
  System.DateUtils,

  Grijjy.Bson.IO,
  Grijjy.Scram;

type
  TMsgHeader = packed record
    MessageLength: Int32;
    RequestID: Int32;
    ResponseTo: Int32;
    OpCode: Int32;
    function NewFormat: Boolean; // true if opcode = op_msg
    function ValidResponseHeader: Boolean;
  end;

  PMsgHeader = ^TMsgHeader;

type
  // The OP_REPLY message is sent by the database in response to an
  // OP_QUERY or OP_GET_MORE message.
  // The format of an OP_REPLY message is:

  TOpReplyHeader = packed record
    Header: TMsgHeader;
    ResponseFlags: Int32; // bit vector
    CursorId: Int64; // cursor id if client needs to do get more's
    StartingFrom: Int32; // where in the cursor this reply is starting
    NumberReturned: Int32; // number of documents in the reply
    { Documents: Documents }
  end;

  POpReplyHeader = ^TOpReplyHeader;

  TOPMSGHeader = packed record
    Header: TMsgHeader;
    flagbits: TgoMongoMsgFlags; // 32 bits because msgfExhaustAllowed = 16
  end;

  POpMsgHeader = ^TOPMSGHeader;

  tMsgPayload = class
  const
    EmptyDocSize = 5;
    // class procedure Overflow;
    class function ReadBsonDoc(out Bson: TBytes; buffer: Pointer; BytesAvail: Integer; var BytesRead: Integer): Boolean;

    class function DecodeSequence(SeqStart: Pointer; SizeAvail: Integer; var SizeRead: Integer; var PayloadType: Byte; var Name: string;
      var data: TArray<TBytes>): Boolean;
  end;

  { Implements IgoMongoReply }
  TgoMongoReply = class(TInterfacedObject, IgoMongoReply)
  private
    FHeader: TOpReplyHeader;
    FDocuments: TArray<TBytes>;
    FDocumentTypes: tArray<Byte>;
    FDocumentNames: tarray<String>;
    fFirstdoc: TgoBsonDocument;
  protected
    { IgoMongoReply }
    function _GetResponseFlags: TgoMongoResponseFlags;
    function _GetCursorId: Int64;
    function _GetStartingFrom: Integer;
    function _GetResponseTo: Integer;
    function _GetDocuments: TArray<TBytes>;
    function _GetDocumentNames: TArray<String>;
    function _GetDocumentTypes: TArray<byte>;

    function _FirstDoc: TgoBsonDocument;
  public
    constructor Create(const ABuffer: TBytes; const ASize: Integer);
  end;

type
  { Implements IgoMongoReply }
  TgoMongoMsgReply = class(TInterfacedObject, IgoMongoReply)
  private
    FHeader: TOPMSGHeader;
    FDocuments: TArray<TBytes>;
    FDocumentTypes: tArray<Byte>;
    FDocumentNames: tarray<String>;
    fFirstdoc: TgoBsonDocument;
  protected
    { IgoMongoReply }
    function _GetResponseFlags: TgoMongoResponseFlags;
    function _GetCursorId: Int64;
    function _GetStartingFrom: Integer;
    function _GetResponseTo: Integer;
    function _GetDocuments: TArray<TBytes>;
    function _GetDocumentNames: TArray<String>;
    function _GetDocumentTypes: TArray<byte>;
    function _FirstDoc: TgoBsonDocument;
  public
    constructor Create(const ABuffer: TBytes; const ASize: Integer);
  end;

  { TgoMongoProtocol }

class constructor TgoMongoProtocol.Create;
begin
  FClientSocketManager := TgoClientSocketManager.Create(TgoSocketOptimization.Scale, TgoSocketPoolBehavior.PoolAndReuse);
end;

class destructor TgoMongoProtocol.Destroy;
begin
  FreeAndNil(FClientSocketManager);
end;

function TgoMongoProtocol.saslStart(const APayload: string): IgoMongoReply;
var
  Writer: IgoBsonWriter;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('saslStart', 1);
  if SupportsOpMsg then
    writer.WriteString('$db',FSettings.AuthDatabase);

  if FSettings.AuthMechanism = TgoMongoAuthMechanism.SCRAM_SHA_1 then
    Writer.WriteString('mechanism', 'SCRAM-SHA-1')
  else
    Writer.WriteString('mechanism', 'SCRAM-SHA-256');

  Writer.WriteName('payload');
  Writer.WriteBinaryData(TgoBsonBinaryData.Create(TEncoding.Utf8.GetBytes(APayload)));
  Writer.WriteInt32('autoAuthorize', 1);
  Writer.WriteEndDocument;

 if SupportsOpMsg then
    result:=OpMsg(writer.ToBson,NIL)
  else
    Result := OpQuery(FSettings.AuthDatabase, COLLECTION_COMMAND, [], 0, -1, Writer.ToBson, nil);
end;

function TgoMongoProtocol.saslContinue(const AConversationId: Integer; const APayload: string): IgoMongoReply;
var
  Writer: IgoBsonWriter;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('saslContinue', 1);
  Writer.WriteInt32('conversationId', AConversationId);
  if SupportsOpMsg then
    writer.WriteString('$db', FSettings.AuthDatabase);

  Writer.WriteName('payload');
  Writer.WriteBinaryData(TgoBsonBinaryData.Create(TEncoding.Utf8.GetBytes(APayload)));
  Writer.WriteEndDocument;

 if SupportsOpMsg then
    result:=OpMsg(writer.ToBson,NIL)
  else
    Result := OpQuery(FSettings.AuthDatabase, COLLECTION_COMMAND, [], 0, -1, Writer.ToBson, nil);
end;

function TgoMongoProtocol.Authenticate: Boolean;
var
  Scram: TgoScram;
  ServerFirstMsg, ServerSecondMsg: string;
  ConversationDoc: TgoBsonDocument;
  PayloadBinary: TgoBsonBinaryData;
  ConversationId: Integer;
  Ok: Boolean;
  MongoReply: IgoMongoReply;
begin
  { Reset auth error code }
  FAuthErrorMessage := '';
  FAuthErrorCode := 0;

  { Initialize our Scram helper }
  case FSettings.AuthMechanism of
    TgoMongoAuthMechanism.SCRAM_SHA_1:
      Scram := TgoScram.Create(TgoScramMechanism.SCRAM_SHA_1, FSettings.Username, FSettings.Password);
  else
    Scram := TgoScram.Create(TgoScramMechanism.SCRAM_SHA_256, FSettings.Username, FSettings.Password);
  end;

  try
    { Step 1 }
    Scram.CreateFirstMsg;

    { Start the initial sasl handshake }
    MongoReply := saslStart(SCRAM_GS2_HEADER + Scram.ClientFirstMsg);

    if MongoReply = nil then
      Exit(False);
    ConversationDoc := MongoReply.FirstDoc;
    if ConversationDoc.IsNil then
      Exit(False);

    Ok := ConversationDoc['ok'];
    if not Ok then
    begin
      // {
      // "ok" : 0.0,
      // "errmsg" : "Authentication failed.",
      // "code" : 18,
      // "codeName" : "AuthenticationFailed"
      // }
      FAuthErrorMessage := ConversationDoc['errmsg'];
      FAuthErrorCode := ConversationDoc['code'];
      Exit(False);
    end;

    // {
    // "conversationId" : 1,
    // "done" : false,
    // "payload" : { "$binary" : "a=b,c=d", "$type" : "00" },
    // "ok" : 1.0
    // }
    { The first message from the server to the client }
    PayloadBinary := ConversationDoc['payload'].AsBsonBinaryData;
    ServerFirstMsg := TEncoding.Utf8.GetString(PayloadBinary.AsBytes);
    ConversationId := ConversationDoc['conversationId'];

    { Process the first message from the server to the client }
    Scram.HandleServerFirstMsg(ConversationId, ServerFirstMsg);

    { Step 2 - Send the final client message }
    MongoReply := saslContinue(Scram.ConversationId, Scram.ClientFinalMsg);

    if MongoReply = nil then
      Exit(False);
    ConversationDoc := MongoReply.FirstDoc;
    if ConversationDoc.IsNil then
      Exit(False);

    Ok := ConversationDoc['ok'];
    if not Ok then
    begin
      FAuthErrorMessage := ConversationDoc['errmsg'];
      FAuthErrorCode := ConversationDoc['code'];
      Exit(False);
    end;

    { The second message from the server to the client }
    PayloadBinary := ConversationDoc['payload'].AsBsonBinaryData;
    ServerSecondMsg := TEncoding.Utf8.GetString(PayloadBinary.AsBytes);

    { Process the second message from the server to the client }
    Scram.HandleServerSecondMsg(ServerSecondMsg);

    { Verify that the actual signature matches the servers expected signature }
    if not Scram.ValidSignature then
    begin
      FAuthErrorMessage := 'Server signature does not match';
      FAuthErrorCode := -1;
      Exit(False);
    end;

    { Step 3 - Acknowledge with an empty payload }
    MongoReply := saslContinue(Scram.ConversationId, '');
    if MongoReply = nil then
      Exit(False);
    ConversationDoc := MongoReply.FirstDoc;
    if ConversationDoc.IsNil then
      Exit(False);

    Ok := ConversationDoc['ok'];
    if not Ok then
    begin
      FAuthErrorMessage := ConversationDoc['errmsg'];
      FAuthErrorCode := ConversationDoc['code'];
      Exit(False);
    end;

    Result := (ConversationDoc['done'] = True);
  finally
    Scram.Free;
  end;
end;

function TgoMongoProtocol.Connect: Boolean;
var
  Connection: TgoSocketConnection;

  procedure WaitForConnected;
  var
    Start: TDateTime;
  begin
    Start := Now;
    while (MillisecondsBetween(Now, Start) < FSettings.ConnectionTimeout) and (FConnection.State <> TgoConnectionState.Connected) do
      Sleep(5);
  end;

begin
  FConnectionLock.Acquire;
  try
    Connection := FConnection;
    FConnection := FClientSocketManager.Request(FHost, FPort);
    if Assigned(FConnection) then
    begin
      FConnection.OnConnected := SocketConnected;
      FConnection.OnDisconnected := SocketDisconnected;
      FConnection.OnRecv := SocketRecv;
    end;
  finally
    FConnectionLock.Release;
  end;

  { Release the last connection }
  if (Connection <> nil) then
    FClientSocketManager.Release(Connection);

  { Shutting down }
  if not Assigned(FConnection) then
    Exit(False);

  Result := (ConnectionState = TgoConnectionState.Connected);
  if (not Result) then
  begin
    FConnectionLock.Acquire;
    try
      { Enable or disable Tls support }
      FConnection.SSL := FSettings.Secure;

      { Pass host name for Server Name Indication (SNI) for Tls }
      if FConnection.SSL then
      begin
        FConnection.OpenSSL.Host := FHost;
        FConnection.OpenSSL.Port := FPort;
      end;

      { Apply X.509 certificate }
      FConnection.Certificate := FSettings.Certificate;
      FConnection.PrivateKey := FSettings.PrivateKey;
      FConnection.Password := FSettings.PrivateKeyPassword;

      if FConnection.Connect then
        WaitForConnected;
    finally
      FConnectionLock.Release;
    end;

    if ConnectionState <> TgoConnectionState.Connected then
      Exit(False);

    Result := True;
  end;

  { Always check this, because credentials may have changed }
  if FSettings.AuthMechanism <> TgoMongoAuthMechanism.None then
  begin
    { SCRAM Authenticate }
    if not Authenticate then
      raise EgoMongoDBConnectionError.Create(Format(RS_MONGODB_AUTHENTICATION_ERROR, [FAuthErrorCode, FAuthErrorMessage]));
  end;
end;

function TgoMongoProtocol.ConnectionState: TgoConnectionState;
begin
  FConnectionLock.Acquire;
  try
    if (FConnection <> nil) then
      Result := FConnection.State
    else
      Result := TgoConnectionState.Disconnected;
  finally
    FConnectionLock.Release;
  end;
end;

constructor TgoMongoProtocol.Create(const AHost: string; const APort: Integer; const ASettings: TgoMongoProtocolSettings);
begin
  Assert(AHost <> '');
  Assert(APort <> 0);
  inherited Create;
  FHost := AHost;
  FPort := APort;
  FSettings := ASettings;
  FConnectionLock := TCriticalSection.Create;
  FRepliesLock := TCriticalSection.Create;
  FRecvBufferLock := TCriticalSection.Create;
  FCompletedReplies := TDictionary<Integer, IgoMongoReply>.Create;
  FPartialReplies := TDictionary<Integer, TDateTime>.Create;
  SetLength(FRecvBuffer, RECV_BUFFER_SIZE);
  InitialHandshake;
end;

destructor TgoMongoProtocol.Destroy;
var
  Connection: TgoSocketConnection;
begin
  if (FConnectionLock <> nil) then
  begin
    FConnectionLock.Acquire;
    try
      Connection := FConnection;
      FConnection := nil;
    finally
      FConnectionLock.Release;
    end;
  end
  else
  begin
    Connection := FConnection;
    FConnection := nil;
  end;

  if (Connection <> nil) and (FClientSocketManager <> nil) then
    FClientSocketManager.Release(Connection);

  if (FRepliesLock <> nil) then
  begin
    FRepliesLock.Acquire;
    try
      FCompletedReplies.Free;
      FPartialReplies.Free;
    finally
      FRepliesLock.Release;
    end;
  end;

  FRepliesLock.Free;
  FConnectionLock.Free;
  FRecvBufferLock.Free;
  inherited;
end;

class function tMsgPayload.ReadBsonDoc(out Bson: TBytes; buffer: Pointer; BytesAvail: Integer; var BytesRead: Integer): Boolean;
var
  DocSize: Integer;
begin
  BytesRead := 0;
  SetLength(Bson, 0);
  Result := False;
  if BytesAvail >= EmptyDocSize then
  begin
    move(buffer^, DocSize, sizeof(Integer)); // read integer into "BytesRead"
    if (BytesAvail >= DocSize) and (DocSize >= EmptyDocSize) then // buffer is big enough?
    begin
      BytesRead := DocSize;
      SetLength(Bson, DocSize);
      move(buffer^, Bson[0], DocSize);
      Result := True; // OK
    end;
  end;
end;

class function tMsgPayload.DecodeSequence(SeqStart: Pointer; SizeAvail: Integer; var SizeRead: Integer; var PayloadType: Byte;
  var Name: string; var data: TArray<TBytes>): Boolean;

{ SeqStart: Start of the section, first byte that follows is the PayloadType
  SizeAvail: Available size from SeqStart to the end of the buffer
  if result=True:
  -> SizeRead:  the size of the processed section.
  -> PayloadType: the decoded payload type
  -> Name: The name of the payload, if PayloadType=1
  -> Data: An array of BSON docs.
}
var
  PayloadSize, offs, PayloadStart: Integer;
  c: ansichar;
  Cstring: utf8string;

  function cursor: Pointer;
  begin
    Result := Pointer(intptr(SeqStart) + offs);
  end;

  procedure read(var output; bytes: Integer); // Simulate simple memory stream
  begin
    move(cursor^, output, bytes);
    inc(offs, bytes);
  end;

  procedure peek(var output; bytes: Integer); // Simulate simple memory stream
  var
    startoffs: Integer;
  begin
    startoffs := offs;
    read(output, bytes);
    offs := startoffs;
  end;

  function BufLeft: Integer;
  begin
    Result := SizeAvail - offs;
  end;

  function PayloadLeft: Integer;
  var
    PayloadProcessed: Integer;
  begin
    PayloadProcessed := offs - PayloadStart;
    Result := PayloadSize - PayloadProcessed;
  end;

  function AppendDoc(var AData: TArray<TBytes>): Boolean;
  var
    tb: TBytes;
    bRead: Integer;
  begin
    Result := ReadBsonDoc(tb, cursor, PayloadLeft, bRead);
    if Result then
    begin
      SetLength(AData, length(AData) + 1);
      AData[high(AData)] := tb;
      inc(offs, bRead); // acknowledge read
    end;
  end;

begin
  Result := False;
  name := '';
  offs := 0;
  SizeRead := 0;
  SetLength(data, 0);
  Cstring := '';
  PayloadSize := 0;

  if BufLeft >= (1 + EmptyDocSize) then // minimum size is 1 byte (payloadtype) + empty bson doc
  begin
    read(PayloadType, 1);
    PayloadStart := offs; // to count how many bytes were processed from THIS point
    if (PayloadType in [0, 1]) then
    begin
      peek(PayloadSize, sizeof(PayloadSize)); // Peek the payload size counter
      if (BufLeft >= PayloadSize) and (PayloadSize >= 0) then // Plausibility check
      begin
        case PayloadType of
          0:
            Result := AppendDoc(data); // Type 0: pull in ONE BSON doc

          1: // Type 1: payload with string header, having [zero or more] BSON docs
            begin
              inc(offs, sizeof(Integer)); // jump over payload counter - we already have it
              // Read string header
              while PayloadLeft > 0 do
              begin
                read(c, 1);
                if c <> #0 then
                  break;
                Cstring := Cstring + c;
              end; // while
              name := string(Cstring);

              Result := True; // the specs say "0 or more" BSON documents, so 0 is acceptable

              // pull in as many docs as allowed
              while PayloadLeft > 0 do
              begin
                if not AppendDoc(data) then
                  break;
              end; // while
            end; // case 1
        end; // case
      end; // if
    end; // if PayloadType
  end; // if bufleft

  if Result then
    SizeRead := sizeof(Byte) + PayloadSize; // Should be identical with offs

end;

procedure TgoMongoProtocol.InitialHandshake;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  Doc: TgoBsonDocument;
  Databases: TgoBsonArray;
  Value: TgoBsonValue;
  I: Integer;
begin
  if FMaxWireVersion = 0 then
  begin
    FMaxWireVersion := 1;
    try
      // https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/handshake.rst

      { isMaster is the deprecated LEGACY version of the hello command. }

      Writer := TgoBsonWriter.Create;
      Writer.WriteStartDocument;
      Writer.WriteInt32('isMaster', 1);
      Writer.WriteEndDocument;
      //LEGACY OpQuery
      Reply := OpQuery(DB_ADMIN, COLLECTION_COMMAND, [], 0, -1, Writer.ToBson, nil);
      if Assigned(Reply) then
      begin
        Doc := Reply.FirstDoc;
        if not Doc.IsNil then
        begin
          FMaxWireVersion := Doc['maxWireVersion'].AsInteger;
          FMinWireVersion := Doc['minWireVersion'].AsInteger;
        end;
      end;
    except
      // ignore
    end;
    // "hello" is the modern and preferred command. It must always be issued using the
    // op_msg protocol.

  end;
end;

function TgoMongoProtocol.IsConnected: Boolean;
begin
  Result := (ConnectionState = TgoConnectionState.Connected);
  if (not Result) then
    Result := Connect;
end;

function TgoMongoProtocol.LastPartialReply(const ARequestId: Integer; out ALastRecv: TDateTime): Boolean;
begin
  FRepliesLock.Acquire;
  try
    Result := FPartialReplies.TryGetValue(ARequestId, ALastRecv);
  finally
    FRepliesLock.Release;
  end;
end;

function TgoMongoProtocol.OpGetMore(const ADatabaseName, ACollectionName: string; const ANumberToReturn: Integer; const ACursorId: Int64)
  : IgoMongoReply;
{ https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-get-more }
var
  Header: TMsgHeader;
  pHeader: PMsgHeader;
  data: tgoByteBuffer;
  I: Integer;
  FullCollectionName: utf8string;
begin
  FullCollectionName := utf8string(ADatabaseName + '.' + ACollectionName);
  Header.RequestID := AtomicIncrement(FNextRequestId);
  Header.ResponseTo := 0;
  Header.OpCode := OP_GET_MORE;
  data := tgoByteBuffer.Create;
  try
    data.AppendBuffer(Header, sizeof(TMsgHeader));
    I := 0;
    data.AppendBuffer(I, sizeof(Int32)); // Reserved
    data.AppendBuffer(FullCollectionName[low(utf8string)], length(FullCollectionName) + 1);
    data.AppendBuffer(ANumberToReturn, sizeof(Int32));
    data.AppendBuffer(ACursorId, sizeof(Int64));
    pHeader := @data.buffer[0];
    pHeader.MessageLength := data.Size;
    Send(data.ToBytes);
  finally
    data.Free;
  end;
  Result := WaitForReply(Header.RequestID);
end;

procedure TgoMongoProtocol.OpKillCursors(const ADatabaseName, ACollectionName: string; const ACursorIds: TArray<Int64>);
{ https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-kill_cursors }
var
  Header: TMsgHeader;
  pHeader: PMsgHeader;
  data: tgoByteBuffer;
  I: Int32;
begin
  if length(ACursorIds) <> 0 then
  begin
    Header.RequestID := AtomicIncrement(FNextRequestId);
    Header.ResponseTo := 0;
    Header.OpCode := OP_KILL_CURSORS;
    data := tgoByteBuffer.Create;
    try
      data.AppendBuffer(Header, sizeof(TMsgHeader));
      I := 0;
      data.AppendBuffer(I, sizeof(Int32)); // Reserved
      I := length(ACursorIds);
      data.AppendBuffer(I, sizeof(Int32)); // Number of cursors to delete
      for I := 0 to high(ACursorIds) do
        data.AppendBuffer(ACursorIds[I], sizeof(Int64));
      pHeader := @data.buffer[0];
      pHeader.MessageLength := data.Size;
      Send(data.ToBytes);
    finally
      data.Free;
    end;
    // The OP_KILL_CURSORS from wire protocol 3.0 does NOT return a result.
  end;
end;

function TgoMongoProtocol.OpMsg(const ParamType0: TBytes; const ParamsType1: TArray<tgoPayloadType1>; NoResponse: Boolean = False)
  : IgoMongoReply;
var
  H: TOPMSGHeader;
  pH: POpMsgHeader;
  data: tgoByteBuffer;
  I: Integer;
  T: TBytes;
  paramtype: Byte;
begin
  H.Header.RequestID := AtomicIncrement(FNextRequestId);
  H.Header.ResponseTo := 0;
  H.Header.OpCode := OP_MSG;
  H.flagbits := [];
  if NoResponse then
    H.flagbits := [TGoMongoMsgFlag.msgfMoreToCome];

  data := tgoByteBuffer.Create;
  try
    data.AppendBuffer(H, sizeof(H));

    // Append parameter of type 0
    paramtype := 0;
    data.Append(paramtype);

    // Every op_msg MUST have ONE section of payload type 0.
    // this is the standard command document, like
    // {"insert": "collection"}, plus write concern and other
    // command arguments. It does not have to contain a BSON
    // array of documents, however. These may be outsourced
    // to parameters of type 1.
    data.Append(ParamType0);

    // Document Stream: the big win comes here.
    // This is the same simple format as Ye Olde Wire Protocol,
    // which is very efficient to assemble and disassemble.

    for I := 0 to high(ParamsType1) do
      ParamsType1[I].WriteTo(data);

    // write optional checksum here
    // ...
    // update bytecount in header
    pH := @data.buffer[0];
    pH.Header.MessageLength := data.Size;
    Send(data.ToBytes);
  finally
    data.Free;
  end;

  if NoResponse then
    Result := nil
  else
    Result := WaitForReply(H.Header.RequestID);
end;

function TgoMongoProtocol.OpQuery(const ADatabase, ACollectionName: string; const AFlags: TgoMongoQueryFlags;
  const ANumberToSkip, ANumberToReturn: Integer; const AQuery, AReturnFieldsSelector: TBytes): IgoMongoReply;
{ https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#wire-op-query }
var
  Header: TMsgHeader;
  pHeader: PMsgHeader;
  data: tgoByteBuffer;
  I: Int32;
  FullCollectionName: utf8string;
begin
  FullCollectionName := utf8string(ADatabase + '.' + ACollectionName);
  Header.RequestID := AtomicIncrement(FNextRequestId);
  Header.ResponseTo := 0;
  Header.OpCode := OP_QUERY;
  data := tgoByteBuffer.Create;
  try
    data.AppendBuffer(Header, sizeof(TMsgHeader));
    I := Byte(AFlags) or Byte(FSettings.QueryFlags);
    data.AppendBuffer(I, sizeof(Int32));
    data.AppendBuffer(FullCollectionName[low(utf8string)], length(FullCollectionName) + 1);
    data.AppendBuffer(ANumberToSkip, sizeof(Int32));
    data.AppendBuffer(ANumberToReturn, sizeof(Int32));
    if (AQuery <> nil) then
      data.Append(AQuery)
    else
      data.Append(EMPTY_DOCUMENT);
    if (AReturnFieldsSelector <> nil) then
      data.Append(AReturnFieldsSelector);
    pHeader := @data.buffer[0];
    pHeader.MessageLength := data.Size;
    Send(data.ToBytes);
  finally
    data.Free;
  end;
  Result := WaitForReply(Header.RequestID);
end;

function TgoMongoProtocol.HaveReplyMsgHeader(out AMsgHeader): Boolean;
begin
  Result := HaveReplyMsgHeader(AMsgHeader, FRecvBuffer, FRecvSize);
end;

function TgoMongoProtocol.HaveReplyMsgHeader(out AMsgHeader; tb: TBytes; Size: Integer): Boolean;
begin
  Result := (Size >= sizeof(TMsgHeader));
  if (Result) then
    move(tb[0], AMsgHeader, sizeof(TMsgHeader));
end;

function TgoMongoProtocol.OpReplyValid(out NewFormat: Boolean; out AIndex: Integer): Boolean;
// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#wire-op-reply
var
  MsgHeader: TMsgHeader; // for format detection
  Header: POpReplyHeader;
  DocSize: Int32;
  ToCount: Integer;

  function HaveBytes(aBytes: Integer): Boolean;
  begin
    Result := (FRecvSize >= (AIndex + aBytes))
  end;

begin
  Result := False;
  AIndex := 0;
  NewFormat := False;

  // Detect if the reply is of the OLD (OP_reply) or NEW (OP_MSG) type
  // and detect if enough bytes were received.

  if HaveReplyMsgHeader(MsgHeader) then
  begin
    NewFormat := MsgHeader.NewFormat;
    if FRecvSize < MsgHeader.MessageLength then
      Exit; // reception incomplete
  end
  else
    Exit; // Not enough bytes received

  (* The rest of this code is merely counting if it has all documents that
    the header says it must have. That is NOT a good verification and it can
    NOT recover from any data errors. It is not able to remove a faulty record
    from the input  buffer and can not "seek" to the next record *)

  case NewFormat of
    True:
      begin
        { TODO : Needs to be better }
        Result := True;
        AIndex := MsgHeader.MessageLength;
      end;

    False:
      begin
        if (FRecvSize >= sizeof(TOpReplyHeader)) then
        begin
          Header := @FRecvBuffer[0];
          AIndex := sizeof(TOpReplyHeader); // Position just after the header
          if (Header.NumberReturned = 0) then
          begin
            Result := True; { no documents, ok }
          end
          else
          begin
            { Make sure we have all the documents }
            ToCount := Header.NumberReturned;
            repeat
              if HaveBytes(sizeof(Int32)) then
              begin
                // consume 4 bytes "size"
                move(FRecvBuffer[AIndex], DocSize, sizeof(Int32));
                if HaveBytes(DocSize) then
                begin
                  Dec(ToCount); // OK, have this document
                  AIndex := AIndex + DocSize; // move pointer to next document
                end
                else
                  break;
              end
              else
                break;
            until (ToCount = 0);
            Result := (ToCount = 0); { all documents, ok }
            if Result then
              AIndex := MsgHeader.MessageLength; // essential, just in case a crc is appended to the end
          end;
        end
        else
          Result := False;
      end;
  end; // case
end;


// proc

procedure TgoMongoProtocol.Send(const AData: TBytes);
begin
  if IsConnected then
  begin
    FConnectionLock.Acquire;
    try
      if (FConnection <> nil) then
        FConnection.Send(AData);
    finally
      FConnectionLock.Release;
    end;
  end;
end;

procedure TgoMongoProtocol.SocketConnected;
begin
  { Not interested (yet) }
end;

procedure TgoMongoProtocol.SocketDisconnected;
begin
  { Not interested (yet) }
end;

procedure TgoMongoProtocol.SocketRecv(const ABuffer: Pointer; const ASize: Integer);
var
  MongoReply: IgoMongoReply;
  Index: Integer;
  MsgHeader: TMsgHeader;
  NewFormat: Boolean;
begin
  FRecvBufferLock.Enter;
  try
    { Expand the buffer if we are at capacity }
    if (FRecvSize + ASize >= length(FRecvBuffer)) then
      SetLength(FRecvBuffer, (FRecvSize + ASize) * 2);

    { Append the new buffer }
    move(ABuffer^, FRecvBuffer[FRecvSize], ASize);
    FRecvSize := FRecvSize + ASize;

    { Is there one or more valid replies pending? }
    while True do
    begin
      if OpReplyValid(NewFormat, index) then
      begin
        if not NewFormat then
          MongoReply := TgoMongoReply.Create(FRecvBuffer, index)
        else
          MongoReply := TgoMongoMsgReply.Create(FRecvBuffer, index);

        FRepliesLock.Acquire;
        try
          { Remove the partial reply timestamp }
          FPartialReplies.Remove(MongoReply.ResponseTo);

          { Add the completed reply to the dictionary }
          FCompletedReplies.Add(MongoReply.ResponseTo, MongoReply);
        finally
          FRepliesLock.Release;
        end;

        { remove the processed bytes, if needed }
        if (index = FRecvSize) then
          FRecvSize := 0
        else
          move(FRecvBuffer[index], FRecvBuffer[0], FRecvSize - index);
      end
      else
      begin
        { the partial reply has grown, Update the partial reply timestamp }
        if HaveReplyMsgHeader(MsgHeader) and MsgHeader.ValidResponseHeader then
        begin
          FRepliesLock.Acquire;
          try
            FPartialReplies.AddOrSetValue(MsgHeader.ResponseTo, Now);
          finally
            FRepliesLock.Release;
          end;
        end;
        break;
      end;
    end;
  finally
    FRecvBufferLock.Leave;
  end;
end;

function TgoMongoProtocol.SupportsOpMsg: Boolean;
begin
  Result := (FMaxWireVersion >= 6);
end;

function TgoMongoProtocol.TryGetReply(const ARequestId: Integer; out AReply: IgoMongoReply): Boolean;
begin
  FRepliesLock.Acquire;
  try
    Result := FCompletedReplies.TryGetValue(ARequestId, AReply);
  finally
    FRepliesLock.Release;
  end;
end;

function TgoMongoProtocol.WaitForReply(const ARequestId: Integer): IgoMongoReply;
var
  _Now, _Init, LastRecv: TDateTime;
begin
  Result := nil;
  _Init := Now;
  while (ConnectionState = TgoConnectionState.Connected) and (not TryGetReply(ARequestId, Result)) do
  begin
    _Now := Now;
    if LastPartialReply(ARequestId, LastRecv) then
    begin
      // give reply some time to become complete
      if (MillisecondsBetween(_Now, LastRecv) > FSettings.ReplyTimeout) then
        break;
    end
    else // reply is nowhere to be found
      if MillisecondsBetween(_Now, _Init) > FSettings.ReplyTimeout then
        break;
    Sleep(5);
  end;

  if (Result = nil) then
    TryGetReply(ARequestId, Result);

  RemoveReply(ARequestId);

  if (Result = nil) then
    Recover; // There could be trash in the input buffer, blocking the system
end;

procedure TgoMongoProtocol.Recover;
var
  Index: Integer;
  MsgHeader: TMsgHeader;
  NewFormat: Boolean;
begin
  { remove trash from the reception buffer }
  FRecvBufferLock.Enter;
  try
    if (FRecvSize > 0) and not OpReplyValid(NewFormat, index) then
    begin
      // if it begins with a valid response header, remove its statistics
      if HaveReplyMsgHeader(MsgHeader) then
      begin
        if MsgHeader.ValidResponseHeader then
          RemoveReply(MsgHeader.RequestID);
      end;
      // Now clear the buffer
      FRecvSize := 0;
    end;
  finally
    FRecvBufferLock.Leave;
  end;
end;

procedure TgoMongoProtocol.RemoveReply(const ARequestId: Integer);
begin
  FRepliesLock.Acquire;
  try
    FPartialReplies.Remove(ARequestId);
    FCompletedReplies.Remove(ARequestId);
  finally
    FRepliesLock.Release;
  end;
end;

{ OLD FORMAT TgoMongoReply }

constructor TgoMongoReply.Create(const ABuffer: TBytes; const ASize: Integer);
var
  I, j, Index, Count: Integer;
  Size: Int32;
  Document: TBytes;
begin
  inherited Create;
  fFirstdoc.SetNil;
  if (ASize >= sizeof(TOpReplyHeader)) then
  begin
    move(ABuffer[0], FHeader, sizeof(FHeader));
    if (FHeader.NumberReturned > 0) then
    begin
      index := sizeof(FHeader);

      Count := 0;
      SetLength(FDocuments, FHeader.NumberReturned);
      SetLength(FDocumentNames, FHeader.NumberReturned); //filled with empty strings - is as-desired
      SetLength(FDocumentTypes, FHeader.NumberReturned); //filled with 0s - is as-desired

      for I := 0 to FHeader.NumberReturned - 1 do
      begin
        move(ABuffer[index], Size, sizeof(Int32));
        if (ASize < (index + Size)) then
          break;
        SetLength(Document, Size);
        move(ABuffer[index], Document[0], Size);
        FDocuments[Count] := Document;
        inc(index, Size);
        inc(Count);
      end;

      SetLength(FDocuments, Count);
    end;
  end
  else
    FHeader.CursorId := -1;
end;

function TgoMongoReply._FirstDoc: TgoBsonDocument;
begin
  if fFirstdoc.IsNil then
  begin
    if length(FDocuments) > 0 then
      fFirstdoc := TgoBsonDocument.Load(FDocuments[0]);
  end;
  Result := fFirstdoc;
end;

function TgoMongoReply._GetCursorId: Int64;
begin
  Result := FHeader.CursorId;
end;

function TgoMongoReply._GetDocumentNames: TArray<String>;
begin
  result:=fdocumentnames;
end;

function TgoMongoReply._GetDocuments: TArray<TBytes>;
begin
  Result := FDocuments;
end;

function TgoMongoReply._GetDocumentTypes: TArray<byte>;
begin
  result:=fdocumenttypes;
end;

function TgoMongoReply._GetResponseFlags: TgoMongoResponseFlags;
begin
  Byte(Result) := FHeader.ResponseFlags;
end;

function TgoMongoReply._GetResponseTo: Integer;
begin
  Result := FHeader.Header.ResponseTo;
end;

function TgoMongoReply._GetStartingFrom: Integer;
begin
  Result := FHeader.StartingFrom;
end;

{ TMsgHeader }

function TMsgHeader.ValidResponseHeader: Boolean;
begin
  { VERY basic format detection, but better than nothing }
  Result := (self.OpCode = 1) or (self.OpCode = 2013);
end;

function TMsgHeader.NewFormat: Boolean; // New wire protocol
begin
  Result := (self.OpCode = 2013);
end;

{ TgoMongoMsgReply }

constructor TgoMongoMsgReply.Create(const ABuffer: TBytes; const ASize: Integer);
var
  I, j, Index, Count, k: Integer;
  Size: Int32;
  DocBuf: TArray<TBytes>;
  data: Pointer;
  ofs, Avail, SizeRead, NewSize: Integer;
  PayloadType: Byte;
  Name: string;
begin
  inherited Create;
  fFirstdoc.SetNil;
  if (ASize >= sizeof(TOpReplyHeader)) then
  begin
    move(ABuffer[0], FHeader, sizeof(FHeader));
    ofs := sizeof(FHeader);
    data := @ABuffer[ofs];
    Avail := FHeader.Header.MessageLength - ofs;
    while tMsgPayload.DecodeSequence(data, Avail, SizeRead, PayloadType, name, DocBuf) do
    begin
      k := length(FDocuments);
      NewSize:=k + length(DocBuf);
      SetLength(FDocuments, NewSize);
      Setlength(fDocumentTypes,NewSize);
      Setlength(fDocumentNames,NewSize);

      for I := 0 to high(DocBuf) do
      begin
        FDocuments[k + I] := DocBuf[I];
        fDocumentTypes[k+i]:=PayloadType;
        fDocumentNames[k+i]:=Name;
      end;
      Avail := Avail - SizeRead;
      inc(intptr(data), SizeRead);
    end;
  end;
end;

function TgoMongoMsgReply._FirstDoc: TgoBsonDocument;
begin
  if fFirstdoc.IsNil then
  begin
    if length(FDocuments) > 0 then
      fFirstdoc := TgoBsonDocument.Load(FDocuments[0]);
  end;
  Result := fFirstdoc;
end;

function TgoMongoMsgReply._GetCursorId: Int64;
var
  Doc: TgoBsonDocument;
  cursor: TgoBsonValue;
begin
  Result := 0;
  Doc := _FirstDoc;
  if not Doc.IsNil then
  begin
    cursor := Doc['cursor'];
    if not cursor.IsNil then
      Result := cursor.AsBsonDocument['id'];
  end;
end;

function TgoMongoMsgReply._GetDocumentNames: TArray<String>;
begin
  Result:=fDocumentNames;
end;

function TgoMongoMsgReply._GetDocuments: TArray<TBytes>;
begin
  Result := FDocuments;
end;

function TgoMongoMsgReply._GetDocumentTypes: TArray<byte>;
begin
  Result:=fDocumentTypes;
end;

function TgoMongoMsgReply._GetResponseFlags: TgoMongoResponseFlags;
begin
  Result := [];
  { TODO : noch tun }
end;

function TgoMongoMsgReply._GetResponseTo: Integer;
begin
  Result := FHeader.Header.ResponseTo;
end;

function TgoMongoMsgReply._GetStartingFrom: Integer;
begin
  result:=-1; //obsolete
end;

{ tgoPayloadType1 }

procedure tgoPayloadType1.WriteTo(buffer: tgoByteBuffer);
{ Convert an arbitrary number of bson documents into a MSG payload of type 1 }
var
  Cstring: utf8string;
  MarkPos, I, SomeInteger: Integer;
  pSize: pInteger;
  PayloadType: Byte;
begin
  PayloadType := 1;
  buffer.Append(PayloadType); // before "size" marker
  MarkPos := buffer.Size; // Position of the "size" marker in the stream
  SomeInteger := 0;
  buffer.Append(SomeInteger); // placeholder for Size
  Cstring := utf8string(name);
  buffer.AppendBuffer(Cstring[low(utf8string)], length(Cstring) + 1); // string plus #0
  if Assigned(Docs) then
    for I := 0 to high(Docs) do
      if Assigned(Docs[I]) then
        buffer.Append(Docs[I]);
  pSize := @buffer.buffer[MarkPos];
  pSize^ := buffer.Size - MarkPos;
end;

initialization

end.
