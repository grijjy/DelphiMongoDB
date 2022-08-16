unit Grijjy.MongoDB.Protocol;
{< Implements the MongoDB Wire Protocol.
   This unit is only used internally. }

{$INCLUDE 'Grijjy.inc'}

interface

uses
  System.SyncObjs,
  System.SysUtils,
  System.Generics.Collections,
  {$IF Defined(MSWINDOWS)}
  Grijjy.SocketPool.Win,
  {$ELSEIF Defined(LINUX)}
  Grijjy.SocketPool.Linux,
  {$ELSE}
    {$MESSAGE Error 'The MongoDB driver is only supported on Windows and Linux'}
  {$ENDIF}
  Grijjy.Bson;

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
    TailableCursor = 1,

    { Allow query of replica slave. Normally these return an error except for
      namespace “local”. }
    SlaveOk = 2,

    { Internal replication use only - driver should not set. }
    OplogRelay = 3,

    { The server normally times out idle cursors after an inactivity period
      (10 minutes) to prevent excess memory use. Set this option to prevent
      that. }
    NoCursorTimeout = 4,

    { Use with TailableCursor. If we are at the end of the data, block for a
      while rather than returning no data. After a timeout period, we do return
      as normal. }
    AwaitData = 5,

    { Stream the data down full blast in multiple “more” packages, on the
      assumption that the client will fully read all data queried. Faster when
      you are pulling a lot of data and know you want to pull it all down.
      Note: the client is not allowed to not read all the data unless it closes the connection. }
    Exhaust = 6,

    { Get partial results from a mongos if some shards are down (instead of
      throwing an error) }
    Partial = 7);
  TgoMongoQueryFlags = set of TgoMongoQueryFlag;

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
  { A reply to a query (see TgoMongoProtocol.OpQuery) }
  IgoMongoReply = interface
  ['{25CEF8E1-B023-4232-BE9A-1FBE9E51CE57}']
    {$REGION 'Internal Declarations'}
    function _GetResponseFlags: TgoMongoResponseFlags;
    function _GetCursorId: Int64;
    function _GetStartingFrom: Integer;
    function _GetResponseTo: Integer;
    function _GetDocuments: TArray<TBytes>;
    {$ENDREGION 'Internal Declarations'}

    { Various reponse flags }
    property ReponseFlags: TgoMongoResponseFlags read _GetResponseFlags;

    { The cursorID that this reply is a part of. In the event that the result
      set of the query fits into one reply message, cursorID will be 0.
      This cursorID must be used in any GetMore messages used to get more data. }
    property CursorId: Int64 read _GetCursorId;

    { Starting position in the cursor.}
    property StartingFrom: Integer read _GetStartingFrom;

    { The identifier of the message that this reply is response to. }
    property ResponseTo: Integer read _GetResponseTo;

    { Raw BSON documents in the reply. }
    property Documents: TArray<TBytes> read _GetDocuments;
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
    PrivateKeyPassword: String;

    { Authentication mechanism }
    AuthMechanism: TgoMongoAuthMechanism;

    { Authentication database }
    AuthDatabase: String;

    { Authentication username }
    Username: String;

    { Authentication password }
    Password: String;
  end;

type
  TgoMongoProtocol = class
  {$REGION 'Internal Declarations'}
  private const
    OP_QUERY = 2004;
    OP_GET_MORE = 2005;
    OP_KILL_CURSORS=2007;
    RECV_BUFFER_SIZE = 32768;
    EMPTY_DOCUMENT: array [0..4] of Byte = (5, 0, 0, 0, 0);
  private class var
    FClientSocketManager: TgoClientSocketManager;
  private
    FHost: String;
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
    FAuthErrorMessage: String;
    FAuthErrorCode: Integer;
  private
    procedure Send(const AData: TBytes);
    function WaitForReply(const ARequestId: Integer): IgoMongoReply;
    function TryGetReply(const ARequestId: Integer; out AReply: IgoMongoReply): Boolean; inline;
    function LastPartialReply(const ARequestID: Integer; out ALastRecv: TDateTime): Boolean;
    function OpReplyValid(out AIndex: Integer): Boolean;
    function OpReplyMsgHeader(out AMsgHeader): Boolean;
  private
    { Authentication }
    function saslStart(const APayload: String): IgoMongoReply;
    function saslContinue(const AConversationId: Integer; const APayload: String): IgoMongoReply;
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
    constructor Create(const AHost: String; const APort: Integer;
      const ASettings: TgoMongoProtocolSettings);
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
    function OpQuery(const AFullCollectionName: String;
      const AFlags: TgoMongoQueryFlags; const ANumberToSkip,
      ANumberToReturn: Integer; const AQuery: TBytes;
      const AReturnFieldsSelector: TBytes = nil): IgoMongoReply;

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

    function OpGetMore(const AFullCollectionName: String;
      const ANumberToReturn: Integer; const ACursorId: Int64): IgoMongoReply;

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
         https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-kill_cursors}

    Procedure OpKillCursors(const ACursorIds: Tarray<Int64>);

  public
    { Authenticate error message if failed }
    property AuthErrorMessage: String read FAuthErrorMessage;

    { Authenticate error code if failed }
    property AuthErrorCode: Integer read FAuthErrorCode;
  end;

resourcestring
  RS_MONGODB_AUTHENTICATION_ERROR = 'Error authenticating [%d] %s';

implementation

uses
  System.DateUtils,
  Grijjy.SysUtils,
  Grijjy.Bson.IO,
  Grijjy.Scram;

type
  TMsgHeader = packed record
    MessageLength: Int32;
    RequestID: Int32;
    ResponseTo: Int32;
    OpCode: Int32;
  end;
  PMsgHeader = ^TMsgHeader;

type
  TOpReplyHeader = packed record
    Header: TMsgHeader;
    ResponseFlags: Int32;
    CursorId: Int64;
    StartingFrom: Int32;
    NumberReturned: Int32;
    { Documents: Documents }
  end;
  POpReplyHeader = ^TOpReplyHeader;

type
  { Implements IgoMongoReply }
  TgoMongoReply = class(TInterfacedObject, IgoMongoReply)
  private
    FHeader: TOpReplyHeader;
    FDocuments: TArray<TBytes>;
  protected
    { IgoMongoReply }
    function _GetResponseFlags: TgoMongoResponseFlags;
    function _GetCursorId: Int64;
    function _GetStartingFrom: Integer;
    function _GetResponseTo: Integer;
    function _GetDocuments: TArray<TBytes>;
  public
    constructor Create(const ABuffer: TBytes; const ASize: Integer);
  end;

{ TgoMongoProtocol }

class constructor TgoMongoProtocol.Create;
begin
  FClientSocketManager := TgoClientSocketManager.Create(
    TgoSocketOptimization.Scale, TgoSocketPoolBehavior.PoolAndReuse);
end;

class destructor TgoMongoProtocol.Destroy;
begin
  FreeAndNil(FClientSocketManager);
end;

function TgoMongoProtocol.saslStart(const APayload: String): IgoMongoReply;
var
  Writer: IgoBsonWriter;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('saslStart', 1);
  if FSettings.AuthMechanism = TgoMongoAuthMechanism.SCRAM_SHA_1 then
    Writer.WriteString('mechanism', 'SCRAM-SHA-1')
  else
    Writer.WriteString('mechanism', 'SCRAM-SHA-256');

  Writer.WriteName('payload');
  Writer.WriteBinaryData(TgoBsonBinaryData.Create(TEncoding.Utf8.GetBytes(APayload)));

  Writer.WriteInt32('autoAuthorize', 1);
  Writer.WriteEndDocument;

  Result := OpQuery(FSettings.AuthDatabase + '.$cmd', [], 0, -1, Writer.ToBson, nil);
end;

function TgoMongoProtocol.saslContinue(const AConversationId: Integer; const APayload: String): IgoMongoReply;
var
  Writer: IgoBsonWriter;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('saslContinue', 1);
  Writer.WriteInt32('conversationId', AConversationId);

  Writer.WriteName('payload');
  Writer.WriteBinaryData(TgoBsonBinaryData.Create(TEncoding.Utf8.GetBytes(APayload)));
  Writer.WriteEndDocument;

  Result := OpQuery(FSettings.AuthDatabase + '.$cmd', [], 0, -1, Writer.ToBson, nil);
end;

function TgoMongoProtocol.Authenticate: Boolean;
var
  Scram: TgoScram;
  ServerFirstMsg, ServerSecondMsg: String;
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

    if MongoReply.Documents = nil then
      Exit(False);

    ConversationDoc := TgoBsonDocument.Load(MongoReply.Documents[0]);
    Ok := ConversationDoc['ok'];
    if not Ok then
    begin
//      {
//        "ok" : 0.0,
//        "errmsg" : "Authentication failed.",
//        "code" : 18,
//        "codeName" : "AuthenticationFailed"
//      }
      FAuthErrorMessage := ConversationDoc['errmsg'];
      FAuthErrorCode := ConversationDoc['code'];
      Exit(False);
    end;

//    {
//      "conversationId" : 1,
//      "done" : false,
//      "payload" : { "$binary" : "a=b,c=d", "$type" : "00" },
//      "ok" : 1.0
//    }
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

    if MongoReply.Documents = nil then
      Exit(False);

    ConversationDoc := TgoBsonDocument.Load(MongoReply.Documents[0]);
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

    if MongoReply.Documents = nil then
      Exit(False);

    ConversationDoc := TgoBsonDocument.Load(MongoReply.Documents[0]);
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
    while (MillisecondsBetween(Now, Start) < FSettings.ConnectionTimeout) and
      (FConnection.State <> TgoConnectionState.Connected)
    do
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

constructor TgoMongoProtocol.Create(const AHost: String; const APort: Integer;
  const ASettings: TgoMongoProtocolSettings);
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

function TgoMongoProtocol.IsConnected: Boolean;
begin
  Result := (ConnectionState = TgoConnectionState.Connected);
  if (not Result) then
    Result := Connect;
end;

function TgoMongoProtocol.LastPartialReply(const ARequestID: Integer;
  out ALastRecv: TDateTime): Boolean;
begin
  FRepliesLock.Acquire;
  try
    Result := FPartialReplies.TryGetValue(ARequestID, ALastRecv);
  finally
    FRepliesLock.Release;
  end;
end;

function TgoMongoProtocol.OpGetMore(const AFullCollectionName: String;
  const ANumberToReturn: Integer; const ACursorId: Int64): IgoMongoReply;
{ https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-get-more }
var
  Header: TMsgHeader;
  Data: TgoByteBuffer;
  I: Integer;
  FullCollectionName:UTF8String;
begin
  FullCollectionName:=UTF8String(AFullCollectionName);
  Header.MessageLength := SizeOf(TMsgHeader) + 16
    + Length(FullCollectionName) + 1;
  Header.RequestID := AtomicIncrement(FNextRequestId);
  Header.ResponseTo := 0;
  Header.OpCode := OP_GET_MORE;

  Data := TgoByteBuffer.Create(Header.MessageLength);
  try
    Data.AppendBuffer(Header, SizeOf(TMsgHeader));
    I := 0;
    Data.AppendBuffer(I, SizeOf(Int32)); // Reserved
    Data.AppendBuffer(FullCollectionName[Low(UTF8String)], Length(FullCollectionName) + 1);
    Data.AppendBuffer(ANumberToReturn, SizeOf(Int32));
    Data.AppendBuffer(ACursorId, SizeOf(Int64));
    Send(Data.ToBytes);
  finally
    Data.Free;
  end;
  Result := WaitForReply(Header.RequestID);
end;

Procedure TgoMongoProtocol.OpKillCursors(const ACursorIds: TArray<Int64>);
{https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-kill_cursors}
var
  Header: TMsgHeader;
  Data: TgoByteBuffer;
  I: Int32;
begin
  if Length(ACursorIds) <> 0 then
  begin
    Header.MessageLength := SizeOf(TMsgHeader) + 2 * SizeOf(Int32) + Length(ACursorIds) * SizeOf(Int64);
    Header.RequestID := AtomicIncrement(FNextRequestId);
    Header.ResponseTo := 0;
    Header.OpCode := OP_KILL_CURSORS;
    Data := TgoByteBuffer.Create(Header.MessageLength);
    try
      Data.AppendBuffer(Header, SizeOf(TMsgHeader));
      I := 0;
      Data.AppendBuffer(I, SizeOf(Int32)); // Reserved
      I := Length(ACursorIds);
      Data.AppendBuffer(I, SizeOf(Int32)); // Number of cursors to delete
      for I := 0 to high(ACursorIds) do
        Data.AppendBuffer(ACursorIds[I], SizeOf(Int64));
      Send(Data.ToBytes);
    finally
      Data.Free;
    end;
    // The OP_KILL_CURSORS from wire protocol 3.0 does NOT return a result.
  end;
end;

function TgoMongoProtocol.OpQuery(const AFullCollectionName: String;
  const AFlags: TgoMongoQueryFlags; const ANumberToSkip,
  ANumberToReturn: Integer; const AQuery,
  AReturnFieldsSelector: TBytes): IgoMongoReply;
{ https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#wire-op-query }
var
  Header: TMsgHeader;
  Data: TgoByteBuffer;
  I: Int32;
  FullCollectionName:UTF8String;
begin
  FullCollectionName:=UTF8String(AFullCollectionName);

  Header.MessageLength := SizeOf(TMsgHeader) + 12
    + Length(FullCollectionName) + 1
    + Length(AQuery) + Length(AReturnFieldsSelector);
  if (AQuery = nil) then
    Inc(Header.MessageLength, Length(EMPTY_DOCUMENT));
  Header.RequestID := AtomicIncrement(FNextRequestId);
  Header.ResponseTo := 0;
  Header.OpCode := OP_QUERY;

  Data := TgoByteBuffer.Create(Header.MessageLength);
  try
    Data.AppendBuffer(Header, SizeOf(TMsgHeader));
    I := Byte(AFlags) or Byte(FSettings.QueryFlags);
    Data.AppendBuffer(I, SizeOf(Int32));
    Data.AppendBuffer(FullCollectionName[Low(UTF8String)], Length(FullCollectionName) + 1);
    Data.AppendBuffer(ANumberToSkip, SizeOf(Int32));
    Data.AppendBuffer(ANumberToReturn, SizeOf(Int32));
    if (AQuery <> nil) then
      Data.Append(AQuery)
    else
      Data.Append(EMPTY_DOCUMENT);
    if (AReturnFieldsSelector <> nil) then
      Data.Append(AReturnFieldsSelector);

    Send(Data.ToBytes);
  finally
    Data.Free;
  end;
  Result := WaitForReply(Header.RequestID);
end;

function TgoMongoProtocol.OpReplyMsgHeader(out AMsgHeader): Boolean;
begin
  Result := (FRecvSize >= SizeOf(TMsgHeader));
  if (Result) then
    Move(FRecvBuffer[0], AMsgHeader, SizeOf(TMsgHeader));
end;

function TgoMongoProtocol.OpReplyValid(out AIndex: Integer): Boolean;
// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#wire-op-reply
var
  Header: POpReplyHeader;
  Size: Int32;
  NumberReturned: Integer;
begin
  AIndex := 0;
  if (FRecvSize >= SizeOf(TOpReplyHeader)) then { minimum size }
  begin
    Header := @FRecvBuffer[0];
    if (Header.NumberReturned = 0) then
    begin
      AIndex := SizeOf(TOpReplyHeader);
      Result := True; { no documents, ok }
    end
    else
    begin
      { Make sure we have all the documents }
      NumberReturned := Header.NumberReturned;
      AIndex := SizeOf(TOpReplyHeader);
      repeat
        if (FRecvSize >= (AIndex + SizeOf(Int32))) then
        begin
          Move(FRecvBuffer[AIndex], Size, SizeOf(Int32));
          if (FRecvSize >= (AIndex + Size)) then
          begin
            Dec(NumberReturned);
            AIndex := AIndex + Size; { next }
          end
          else
            Break;
        end
        else
          Break;
      until (NumberReturned = 0);
      Result := (NumberReturned = 0); { all documents, ok }
    end;
  end
  else
    Result := False;
end;

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

procedure TgoMongoProtocol.SocketRecv(const ABuffer: Pointer;
  const ASize: Integer);
var
  MongoReply: IgoMongoReply;
  Index: Integer;
  MsgHeader: TMsgHeader;
begin
  FRecvBufferLock.Enter;
  try
    { Expand the buffer if we are at capacity }
    if (FRecvSize + ASize >= Length(FRecvBuffer)) then
      SetLength(FRecvBuffer, (FRecvSize + ASize) * 2);

    { Append the new buffer }
    Move(ABuffer^, FRecvBuffer[FRecvSize], ASize);
    FRecvSize := FRecvSize + ASize;

    { Is there one or more valid replies pending? }
    while True do
    begin
      if OpReplyValid(Index) then
      begin
        MongoReply := TgoMongoReply.Create(FRecvBuffer, FRecvSize);

        FRepliesLock.Acquire;
        try
          { Remove the partial reply timestamp }
          FPartialReplies.Remove(MongoReply.ResponseTo);

          { Add the completed reply to the dictionary }
          FCompletedReplies.Add(MongoReply.ResponseTo, MongoReply);
        finally
          FRepliesLock.Release;
        end;

        { Shift the receive buffer, if needed }
        if (Index = FRecvSize) then
          FRecvSize := 0
        else
          Move(FRecvBuffer[Index], FRecvBuffer[0], FRecvSize - Index);
      end
      else
      begin
        { Update the partial reply timestamp }
        if OpReplyMsgHeader(MsgHeader) then
        begin
          FRepliesLock.Acquire;
          try
            FPartialReplies.AddOrSetValue(MsgHeader.ResponseTo, Now);
          finally
            FRepliesLock.Release;
          end;
        end;
        Break;
      end;
    end;
  finally
    FRecvBufferLock.Leave;
  end;
end;

function TgoMongoProtocol.TryGetReply(const ARequestId: Integer;
  out AReply: IgoMongoReply): Boolean;
begin
  FRepliesLock.Acquire;
  try
    Result := FCompletedReplies.TryGetValue(ARequestId, AReply);
  finally
    FRepliesLock.Release;
  end;
end;

function TgoMongoProtocol.WaitForReply(
  const ARequestId: Integer): IgoMongoReply;
var
  LastRecv: TDateTime;
begin
  Result := nil;
  while (ConnectionState = TgoConnectionState.Connected) and
    (not TryGetReply(ARequestID, Result)) do
  begin
    if LastPartialReply(ARequestID, LastRecv) and
      (MillisecondsBetween(Now, LastRecv) > FSettings.ReplyTimeout)
    then
      Break;
    Sleep(5);
  end;

  if (Result = nil) then
    TryGetReply(ARequestId, Result);

  FRepliesLock.Acquire;
  try
    FPartialReplies.Remove(ARequestId);
    FCompletedReplies.Remove(ARequestId);
  finally
    FRepliesLock.Release;
  end;
end;

{ TgoMongoReply }

constructor TgoMongoReply.Create(const ABuffer: TBytes; const ASize: Integer);
var
  I, Index, Count: Integer;
  Size: Int32;
  Document: TBytes;
begin
  inherited Create;
  if (ASize >= SizeOf(TOpReplyHeader)) then
  begin
    FHeader := POpReplyHeader(@ABuffer[0])^;
    if (FHeader.NumberReturned > 0) then
    begin
      Index := SizeOf(TOpReplyHeader);
      Count := 0;
      SetLength(FDocuments, FHeader.NumberReturned);

      for I := 0 to FHeader.NumberReturned - 1 do
      begin
        Move(ABuffer[Index], Size, SizeOf(Int32));
        if (ASize < (Index + Size)) then
          Break;

        SetLength(Document, Size);
        Move(ABuffer[Index], Document[0], Size);
        FDocuments[Count] := Document;
        Inc(Index, Size);
        Inc(Count);
      end;

      SetLength(FDocuments, Count);
    end;
  end
  else
    FHeader.CursorId := -1;
end;

function TgoMongoReply._GetCursorId: Int64;
begin
  Result := FHeader.CursorId;
end;

function TgoMongoReply._GetDocuments: TArray<TBytes>;
begin
  Result := FDocuments;
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

end.
