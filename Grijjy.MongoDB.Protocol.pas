unit Grijjy.MongoDB.Protocol;
{ < Implements the MongoDB Wire Protocol.
  This unit is only used internally. }

{$INCLUDE 'Grijjy.inc'}
{$B-}

interface

uses
  System.Diagnostics,
  System.Math,
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
  Grijjy.MongoDB.Compressors,
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

  tgoMongoReadPreference = (primary = 0, primaryPreferred, secondary, secondaryPreferred, nearest, fromParent = 31);

  { Query flags as used by TgoMongoProtocol.OpQuery }
  TgoMongoQueryFlag = ( // OBSOLETE
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

  TGoMongoMsgFlag = (msgfChecksumPresent, msgfMoreToCome, msgfExhaustAllowed = 16, msgfPadding = 31); // padded to ensure the SET is 32 bits
  TgoMongoMsgFlags = set of TGoMongoMsgFlag; // is 4 bytes in size

type
  { Payload type 1 for OP_MSG }
  tgoPayloadType1 = record
    Name: string;
    Docs: TArray<tBytes>;
    procedure WriteTo(buffer: tgoByteBuffer);
  end;

  { A reply to a query (see TgoMongoProtocol.OpMsg) }

  IgoMongoReply = interface
    ['{25CEF8E1-B023-4232-BE9A-1FBE9E51CE57}']
{$REGION 'Internal Declarations'}
    function _GetResponseTo: Integer;
    function _GetPayload0: TArray<tBytes>; { Every message should have exactly ONE document section of type 0 }
    function _GetPayload1: TArray<tgoPayloadType1>; { Every message can have any number of type 1 sections }
    function _FirstDoc: TgoBsonDocument;
{$ENDREGION 'Internal Declarations'}
    { The identifier of the message that this reply is response to. }
    property ResponseTo: Integer read _GetResponseTo;
    { First BSON document in the reply. Always of payload type 0 }
    property FirstDoc: TgoBsonDocument read _FirstDoc;
    property Payload0: TArray<tBytes> read _GetPayload0;
    property Payload1: TArray<tgoPayloadType1> read _GetPayload1;
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
    QueryFlags: TgoMongoQueryFlags; // OBSOLETE

    GlobalReadPreference: tgoMongoReadPreference;

    { Tls enabled }
    Secure: Boolean;

    { X.509 Certificate in PEM format, if any }
    Certificate: tBytes;

    { X.509 Private key in PEM format, if any }
    PrivateKey: tBytes;

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

    ApplicationName: string;

    UseSnappyCompression: Boolean;
    UseZlibCompression: Boolean;

  end;

const
  OP_MSG = 2013;
  OP_COMPRESSED = 2012;

type
  TgoMongoProtocol = class
{$REGION 'Internal Declarations'}
  type

  private const
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
    FPartialReplies: TDictionary<Integer, tStopWatch>;
    FRepliesLock: TCriticalSection;
    FRecvBuffer: tBytes;
    FRecvSize: Integer;
    FRecvBufferLock: TCriticalSection;
    FAuthErrorMessage: string;
    FAuthErrorCode: Integer;
    FMinWireVersion: Integer;
    FMaxWireVersion: Integer;
    FMaxWriteBatchSize: Integer;
    FMaxMessageSizeBytes: Integer;
    fknowsZlib, fknowsSnappy: Boolean;

  private
    procedure Send(const adata: tBytes);
    procedure Recover;
    function WaitForReply(const ARequestId: Integer): IgoMongoReply;
    function TryGetReply(const ARequestId: Integer; out AReply: IgoMongoReply): Boolean; inline;
    function LastPartialReply(const ARequestId: Integer; out ALastRecv: tStopWatch): Boolean;

    function HaveReplyMsgHeader(out AMsgHeader; tb: tBytes; Size: Integer): Boolean; overload;
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
    procedure UpdateReplyTimeout(const ARequestId: Integer);
    procedure Compress(var Packet: tBytes);
  public
    function ThisMoment: tStopWatch;
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

    function OpMsg(const ParamType0: tBytes; const ParamsType1: TArray<tgoPayloadType1>; NoResponse: Boolean = False)
      : IgoMongoReply; overload;

    function OpMsg(CompressionAllowed: Boolean; const ParamType0: tBytes; const ParamsType1: TArray<tgoPayloadType1>;
      NoResponse: Boolean = False): IgoMongoReply; overload;

    function SupportsOpMsg: Boolean;

  public
    { Authenticate error message if failed }
    property AuthErrorMessage: string read FAuthErrorMessage;

    { Authenticate error code if failed }
    property AuthErrorCode: Integer read FAuthErrorCode;
    property MinWireVersion: Integer read FMinWireVersion;
    property MaxWireVersion: Integer read FMaxWireVersion;
    property MaxWriteBatchSize: Integer read FMaxWriteBatchSize write FMaxWriteBatchSize;
    property MaxMessageSizeBytes: Integer read FMaxMessageSizeBytes write FMaxMessageSizeBytes;
    property GlobalReadPreference: tgoMongoReadPreference read FSettings.GlobalReadPreference write FSettings.GlobalReadPreference;
  end;

resourcestring
  RS_MONGODB_AUTHENTICATION_ERROR = 'Error authenticating [%d] %s';

const
  { Maximum number of documents that can be written in bulk at once }
  DEF_MAX_BULK_SIZE = 1000;
  DEF_MAX_MSG_SIZE = 32 * 1024 * 1024;

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
    function ValidOpcode: Boolean; // 2012=op_msg; 2013= compressed
    function Compressed: Boolean;
    function DataStart: Pointer;
    function DataSize: Integer;
  end;

  PMsgHeader = ^TMsgHeader;

type

  TOPMSGHeader = packed record
    Header: TMsgHeader;
    flagbits: TgoMongoMsgFlags; // flagbits is part of the DATA. 32 bits because msgfExhaustAllowed = 16
    // Section+
    // checksum uint32
  end;

  TOPCompressedHeader = packed record
    Header: TMsgHeader; // having message type 2012
    OriginalOpCode: Int32;
    UncompressedSize: Int32;
    CompressorID: Byte;
    function UnCompressedMessageSize: Integer;
    function UnCompressedDataSize: Integer;
    function CompressedDataSize: Integer;
    function DataStart: Pointer;
    // compressed data follows.
    // Consisting of flagbits, followed by Section+ and maybe checksum.
  end;

  POpMsgHeader = ^TOPMSGHeader;
  POPCompressedHeader = ^TOPCompressedHeader;

  tCRC32 = Cardinal;
  tgoPayloadDecodeResult = (pdEOF, pdInvalidPayloadType, pdBufferOverrun, pdOK);

  tMsgPayload = class
  const
    EmptyDocSize = 5;
    MinSequenceSize = 1 + EmptyDocSize;

    // Try to read a BSON document from a buffer. if successful, update "Bytesread".
    // The BSON document itself is not validated.
    // If testmode=true, it is a testrun only, the returned array is empty.
    class function ReadBsonDoc(TestMode: Boolean; out Bson: tBytes; buffer: Pointer; BytesAvail: Integer; var BytesRead: Integer)
      : tgoPayloadDecodeResult;

    // Try to read a TYPE 0 or Type1 payload from a buffer. If successful, update "Bytesread".
    // The BSON documents themselves are not validated.
    // If testmode=true, it is a testrun only, the returned array is empty.
    class function DecodeSequence(TestMode: Boolean; SeqStart: Pointer; SizeAvail: Integer; var SizeRead: Integer; var PayloadType: Byte;
      var Name: string; var data: TArray<tBytes>): tgoPayloadDecodeResult;
  end;

  tgoReplyValidationResult = (rvrOK, // Message decoded successfully
    rvrNoHeader, // Not enough bytes received for a header
    rvrOpcodeInvalid, // Invalid opcode in header
    rvrGrowing, // Message not complete yet, still growing in reception buffer
    rvrCompressorNotSupported, // do not know this compression algorithm
    rvrCompressorError, // unable to decompress, error
    rvrChecksumInvalid, // to be implemented
    rvrDataError); // decoding of message failed, badly formatted or data corruption

  { Implements IgoMongoReply - it is an OP_MSG sent by the server to the client. }
  TgoMongoMsgReply = class(TInterfacedObject, IgoMongoReply)
  private
    FHeader: TOPMSGHeader;
    FPayload0: TArray<tBytes>;
    FPayload1: TArray<tgoPayloadType1>;
    fFirstDoc: TgoBsonDocument;
  protected
    { IgoMongoReply }
    function _GetResponseTo: Integer;
    function _GetPayload0: TArray<tBytes>;
    function _GetPayload1: TArray<tgoPayloadType1>;
    function _FirstDoc: TgoBsonDocument;
  public

    class function ValidateOPMessage(const ABuffer: tBytes; const ASize: Integer; var aSizeRead: Integer; out AReply: IgoMongoReply)
      : tgoReplyValidationResult;

    class function ValidateMessage(const ABuffer: tBytes; const ASize: Integer; var aSizeRead: Integer; out AReply: IgoMongoReply)
      : tgoReplyValidationResult;

    procedure ReadData(const ABuffer: Pointer; const ASize: Integer);
    constructor Create(const ABuffer: Pointer; const ASize: Integer);
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
  buffer.Append(PayloadType); // type comes before before "size" marker
  MarkPos := buffer.Size; // Position of the "size" marker in the stream
  SomeInteger := 0; // placeholder for Size
  buffer.AppendBuffer(SomeInteger, sizeof(Integer));
  Cstring := utf8string(name);
  buffer.AppendBuffer(Cstring[low(utf8string)], length(Cstring) + 1); // string plus #0
  if Assigned(Docs) then
    for I := 0 to high(Docs) do
      if Assigned(Docs[I]) then
        buffer.Append(Docs[I]);
  pSize := @buffer.buffer[MarkPos];
  pSize^ := buffer.Size - MarkPos; // number of bytes written after "markpos"
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
  Writer.WriteString('$db', FSettings.AuthDatabase);
  if FSettings.AuthMechanism = TgoMongoAuthMechanism.SCRAM_SHA_1 then
    Writer.WriteString('mechanism', 'SCRAM-SHA-1')
  else
    Writer.WriteString('mechanism', 'SCRAM-SHA-256');
  Writer.WriteName('payload');
  Writer.WriteBinaryData(TgoBsonBinaryData.Create(TEncoding.Utf8.GetBytes(APayload)));
  Writer.WriteInt32('autoAuthorize', 1);
  Writer.WriteEndDocument;
  result := OpMsg(False, Writer.ToBson, nil)
end;

function TgoMongoProtocol.saslContinue(const AConversationId: Integer; const APayload: string): IgoMongoReply;
var
  Writer: IgoBsonWriter;
begin
  Writer := TgoBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('saslContinue', 1);
  Writer.WriteInt32('conversationId', AConversationId);
  Writer.WriteString('$db', FSettings.AuthDatabase);
  Writer.WriteName('payload');
  Writer.WriteBinaryData(TgoBsonBinaryData.Create(TEncoding.Utf8.GetBytes(APayload)));
  Writer.WriteEndDocument;
  result := OpMsg(False, Writer.ToBson, nil)
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

    result := (ConversationDoc['done'] = True);
  finally
    Scram.Free;
  end;
end;

function TgoMongoProtocol.Connect: Boolean;
var
  Connection: TgoSocketConnection;

  procedure WaitForConnected;
  var
    aNow: tStopWatch;
  begin
    aNow := ThisMoment;
    while (aNow.ElapsedMilliseconds < FSettings.ConnectionTimeout) and (FConnection.State <> TgoConnectionState.Connected) do
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

  result := (ConnectionState = TgoConnectionState.Connected);
  if (not result) then
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

    result := True;
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
      result := FConnection.State
    else
      result := TgoConnectionState.Disconnected;
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
  FMaxWriteBatchSize := DEF_MAX_BULK_SIZE;
  FMaxMessageSizeBytes := DEF_MAX_MSG_SIZE; // 2 x maximum message size of 16 mb
  FSettings := ASettings;
  FConnectionLock := TCriticalSection.Create;
  FRepliesLock := TCriticalSection.Create;
  FRecvBufferLock := TCriticalSection.Create;
  FCompletedReplies := TDictionary<Integer, IgoMongoReply>.Create;
  FPartialReplies := TDictionary<Integer, tStopWatch>.Create;
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

class function tMsgPayload.ReadBsonDoc(TestMode: Boolean; out Bson: tBytes; buffer: Pointer; BytesAvail: Integer; var BytesRead: Integer)
  : tgoPayloadDecodeResult;
var
  DocSize: Integer;
begin
  BytesRead := 0;
  SetLength(Bson, 0);
  result := tgoPayloadDecodeResult.pdEOF; // Assume not enough bytes for minimal bson document
  if BytesAvail >= EmptyDocSize then
  begin
    move(buffer^, DocSize, sizeof(Integer)); // read size of bson document (includes docsize itself)
    if (BytesAvail >= DocSize) and (DocSize >= EmptyDocSize) then // buffer is big enough?
    begin
      result := tgoPayloadDecodeResult.pdOK; // OK
      BytesRead := DocSize;
      if not TestMode then
      begin
        SetLength(Bson, DocSize);
        move(buffer^, Bson[0], DocSize);
      end;
    end
    else // we'd read beyond the end of the buffer, or docsize is invalid
    begin
      result := tgoPayloadDecodeResult.pdBufferOverrun;
    end;
  end;
end;

{ https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#command-arguments-as-payload
  Any unknown Payload Types MUST result in an error and the socket MUST be closed.
  !!! There is no ordering implied by payload types.!!!
  !!! A section with payload type 1 can be serialized before payload type 0!!! }

class function tMsgPayload.DecodeSequence(TestMode: Boolean; SeqStart: Pointer; SizeAvail: Integer; var SizeRead: Integer;
  var PayloadType: Byte; var Name: string; var data: TArray<tBytes>): tgoPayloadDecodeResult;

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
  tempresult: tgoPayloadDecodeResult;

  function cursor: Pointer;
  begin
    result := Pointer(intptr(SeqStart) + offs);
  end;

  procedure read(var Output; bytes: Integer); // Simulate simple memory stream
  begin
    move(cursor^, Output, bytes);
    inc(offs, bytes);
  end;

  procedure peek(var Output; bytes: Integer); // Simulate simple memory stream
  var
    startoffs: Integer;
  begin
    startoffs := offs;
    read(Output, bytes);
    offs := startoffs;
  end;

  function BufLeft: Integer;
  begin
    result := SizeAvail - offs;
  end;

  function PayloadLeft: Integer;
  var
    PayloadProcessed: Integer;
  begin
    PayloadProcessed := offs - PayloadStart;
    result := PayloadSize - PayloadProcessed;
  end;

  function AppendDoc(): tgoPayloadDecodeResult;
  var
    tb: tBytes;
    bRead: Integer;
  begin
    result := ReadBsonDoc(TestMode, tb, cursor, PayloadLeft, bRead);
    if result = tgoPayloadDecodeResult.pdOK then
    begin
      if not TestMode then
      begin
        SetLength(data, length(data) + 1);
        data[high(data)] := tb;
      end;
      inc(offs, bRead); // acknowledge read
    end;
  end;

begin
  result := tgoPayloadDecodeResult.pdEOF;
  name := '';
  offs := 0;
  SizeRead := 0;
  SetLength(data, 0);
  Cstring := '';
  PayloadSize := 0;

  // minimum size for decoding is 6 Bytes (payloadtype + empty bson doc). When we reach the end of the message
  // there may be a CRC which is only 4 bytes. Decoding stops there.

  if BufLeft >= MinSequenceSize then
  begin
    read(PayloadType, 1); // Read the payload type
    PayloadStart := offs; // to facilitate function "PayloadLeft"

    if (PayloadType in [0, 1]) then // disallow other payload types
    begin
      peek(PayloadSize, sizeof(PayloadSize)); // Peek the payload size counter

      if (BufLeft >= PayloadSize) and (PayloadSize >= 0) then // Avoid buffer overrun
      begin
        case PayloadType of

          0: // Type 0: contains ONE BSON doc
            result := AppendDoc();

          1: // Type 1: payload with string header, then zero or more BSON docs
            begin
              inc(offs, sizeof(Integer)); // jump over payload size
              // Read string header - probably just ascii, but allow utf8 anyway
              while PayloadLeft > 0 do
              begin
                read(c, 1);
                if c = #0 then
                  Break;
                SetLength(Cstring, length(Cstring) + 1); // dumb append of byte
                Cstring[length(Cstring)] := c;
              end; // while
              name := string(Cstring);

              result := tgoPayloadDecodeResult.pdOK; // the specs say "0 or more" BSON documents, so 0 is acceptable

              // pull in as many docs as possible
              while PayloadLeft > 0 do
              begin
                tempresult := AppendDoc();
                case tempresult of
                  tgoPayloadDecodeResult.pdOK:
                    Continue; // OK, potentially more documents
                  tgoPayloadDecodeResult.pdEOF:
                    Break; // no more documents, ready
                  tgoPayloadDecodeResult.pdBufferOverrun: // Error
                    begin
                      result := tempresult; // invalidate whole result
                      Break;
                    end;
                else // can't occur
                  Break;
                end;
              end; // while
            end; // case 1
        end; // case
      end // if  bufleft OK
      else if (PayloadSize < 0) or (BufLeft < PayloadSize) then
        result := tgoPayloadDecodeResult.pdBufferOverrun;
    end // if PayloadType OK
    else
      result := tgoPayloadDecodeResult.pdInvalidPayloadType; // unknown PayloadType
  end; // if bufleft

  if result = tgoPayloadDecodeResult.pdOK then
    SizeRead := sizeof(Byte) + PayloadSize; // Should be identical with offs
end;

procedure TgoMongoProtocol.Compress(var Packet: tBytes);
var
  h: PMsgHeader;
  q: POPCompressedHeader;
  DataSize, compressedsize: Integer;
  Output: tBytes;
begin
  h := @Packet[0];
  DataSize := h.DataSize;

  if fknowsSnappy then
  begin
    if tSnappycompressor.Compress(h.DataStart, DataSize, compressedsize, Output) then
    begin
      SetLength(Packet, sizeof(TOPCompressedHeader) + length(Output));
      q := @Packet[0];
      q.OriginalOpCode := q.Header.OpCode;
      q.Header.OpCode := OP_COMPRESSED;
      q.Header.MessageLength := length(Packet);
      q.UncompressedSize := DataSize;
      q.CompressorID := 1; // snappy;
      move(Output[0], q.DataStart^, length(Output));
    end;
  end
  else if fknowsZlib then
  begin
    if tZlibCompressor.Compress(h.DataStart, DataSize, compressedsize, Output) then
    begin
      SetLength(Packet, sizeof(TOPCompressedHeader) + length(Output));
      q := @Packet[0];
      q.OriginalOpCode := q.Header.OpCode;
      q.Header.OpCode := OP_COMPRESSED;
      q.Header.MessageLength := length(Packet);
      q.UncompressedSize := DataSize;
      q.CompressorID := 2; // zlib;
      move(Output[0], q.DataStart^, length(Output));
    end;
  end;

end;

procedure TgoMongoProtocol.InitialHandshake;
var
  Writer: IgoBsonWriter;
  Reply: IgoMongoReply;
  Doc: TgoBsonDocument;
  Compressions: tgoBsonArray;
  value: tgobsonvalue;
  debug: string;
  I: Integer;
begin
  FMaxWireVersion := 1;
  FMinWireVersion := 1;
  FMaxWriteBatchSize := DEF_MAX_BULK_SIZE;
  FMaxMessageSizeBytes := DEF_MAX_MSG_SIZE;
  fknowsZlib := False;
  fknowsSnappy := False;

  try
    // https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/handshake.rst
    { isMaster is the deprecated LEGACY version of the hello command. }
    Writer := TgoBsonWriter.Create;

    // ***************** main doc {
    Writer.WriteStartDocument;
    Writer.WriteInt32('hello', 1);
    Writer.WriteString('$db', DB_ADMIN);

    // client {
    Writer.WriteStartDocument('client');
    if FSettings.ApplicationName <> '' then
    begin
      // application {
      Writer.WriteStartDocument('application');
      Writer.WriteString('name', FSettings.ApplicationName);
      Writer.WriteEndDocument; // application}
    end;

    // driver {
    Writer.WriteStartDocument('driver');
    Writer.WriteString('name', 'Grijjy for Delphi/modified');
    Writer.WriteString('version', '2.0 beta');
    Writer.WriteEndDocument; // driver}

    // os {
    Writer.WriteStartDocument('os');
    case tosVersion.Platform of
      tosVersion.tplatform.pfwindows:
        Writer.WriteString('type', 'Windows');
      tosVersion.tplatform.pfMacOS:
        Writer.WriteString('type', 'Darwin');
    else
      Writer.WriteString('type', 'Linux');
    end;
    Writer.WriteString('name', tosVersion.Name);
    case tosVersion.architecture of
      tosVersion.tarchitecture.arIntelX86:
        Writer.WriteString('architecture', 'x86');
      tosVersion.tarchitecture.arIntelX64:
        Writer.WriteString('architecture', 'x86_64');
      tosVersion.tarchitecture.arArm32:
        Writer.WriteString('architecture', 'arm');
    else
      Writer.WriteString('architecture', 'arm64');
    end;
    Writer.WriteString('version', Format('%d.%d.%d', [tosVersion.Major, tosVersion.Minor, tosVersion.Build]));
    Writer.WriteEndDocument; // os}
    Writer.WriteString('platform', 'Delphi');
    Writer.WriteEndDocument; // client}

    if (FSettings.UseSnappyCompression and Snappy_Implemented) or (FSettings.UseZlibCompression and ZLIB_Implemented) then
    begin
      Writer.WriteStartarray('compression');
      if (FSettings.UseSnappyCompression and Snappy_Implemented) then
        Writer.WriteString('snappy');
      if (FSettings.UseZlibCompression and ZLIB_Implemented) then
        Writer.WriteString('zlib');
      Writer.WriteEndArray { compression };
      Writer.WriteEndDocument; // main doc}
    end;

    Reply := OpMsg(False, Writer.ToBson, nil);
    if Assigned(Reply) then
    begin
      Doc := Reply.FirstDoc;
      if not Doc.IsNil then
      begin
        debug := Doc.ToJson;

        if Doc.Contains('maxWireVersion') then
          FMaxWireVersion := Doc['maxWireVersion'].AsInteger;
        if Doc.Contains('minWireVersion') then
          FMinWireVersion := Doc['minWireVersion'].AsInteger;
        if Doc.Contains('MaxWriteBatchSize') then
          FMaxWriteBatchSize := MAX(Doc['maxWriteBatchSize'].AsInteger, DEF_MAX_BULK_SIZE);
        if Doc.Contains('maxMessageSizeBytes') then
          FMaxMessageSizeBytes := MAX(Doc['maxMessageSizeBytes'].AsInteger, DEF_MAX_MSG_SIZE);

        if Doc.Contains('compression') then
        begin
          Compressions := Doc['compression'].AsBsonArray;
          for I := 0 to Compressions.Count - 1 do
          begin
            value := Compressions[I];
            if value.IsString then
            begin
              if value.AsString = 'snappy' then
                fknowsSnappy := True;
              if value.AsString = 'zlib' then
                fknowsZlib := True;
            end;
          end;
        end;
      end;
    end;
  except
    // ignore
  end;
end;

function TgoMongoProtocol.IsConnected: Boolean;
begin
  result := (ConnectionState = TgoConnectionState.Connected);
  if (not result) then
    result := Connect;
end;

function TgoMongoProtocol.LastPartialReply(const ARequestId: Integer; out ALastRecv: tStopWatch): Boolean;
begin
  FRepliesLock.Acquire;
  try
    result := FPartialReplies.TryGetValue(ARequestId, ALastRecv);
  finally
    FRepliesLock.Release;
  end;
end;

function TgoMongoProtocol.ThisMoment: tStopWatch;
begin
  result := tStopWatch.StartNew;
end;

function TgoMongoProtocol.OpMsg(const ParamType0: tBytes; const ParamsType1: TArray<tgoPayloadType1>; NoResponse: Boolean): IgoMongoReply;
begin
  result := OpMsg(False, ParamType0, ParamsType1, NoResponse);
end;

function TgoMongoProtocol.OpMsg(CompressionAllowed: Boolean; const ParamType0: tBytes; const ParamsType1: TArray<tgoPayloadType1>;
  NoResponse: Boolean = False): IgoMongoReply;
var
  MsgHeader: TOPMSGHeader;
  pHeader: POpMsgHeader;
  data: tgoByteBuffer;
  I: Integer;
  T: tBytes;
  paramtype: Byte;
begin
  if length(ParamType0) = 0 then
    raise EgoMongoDBError.Create('Mandatory document of PayloadType 0 missing in OpMsg');
  MsgHeader.Header.RequestID := AtomicIncrement(FNextRequestId);
  MsgHeader.Header.ResponseTo := 0;
  MsgHeader.Header.OpCode := OP_MSG;
  if NoResponse then
    MsgHeader.flagbits := [TGoMongoMsgFlag.msgfMoreToCome]
  else
    MsgHeader.flagbits := [];

  data := tgoByteBuffer.Create;
  try
    data.AppendBuffer(MsgHeader, sizeof(MsgHeader));
    // Append section of PayloadType 0, that contains the first document.
    // Every op_msg MUST have ONE section of payload type 0.
    // this is the standard command document, like {"insert": "collection"},
    // plus write concern and other command arguments.

    paramtype := 0;
    data.Append(paramtype);
    data.Append(ParamType0);

    // Some parameters may be dis-embedded from the first document and simply appended as sections of Payload Type 1,
    // see https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#command-arguments-as-payload

    for I := 0 to high(ParamsType1) do
      ParamsType1[I].WriteTo(data);

    { TODO : optional Checksum }

    // update message length in header
    pHeader := @data.buffer[0];
    pHeader.Header.MessageLength := data.Size;
    T := data.ToBytes;

    if CompressionAllowed { and (length(T) > 256) } then
      Compress(T);

    Send(T);
  finally
    FreeAndNil(data);
  end;

  if not NoResponse then
    result := WaitForReply(MsgHeader.Header.RequestID)
  else
    result := nil;
end;

function TgoMongoProtocol.HaveReplyMsgHeader(out AMsgHeader): Boolean;
begin
  result := HaveReplyMsgHeader(AMsgHeader, FRecvBuffer, FRecvSize);
end;

function TgoMongoProtocol.HaveReplyMsgHeader(out AMsgHeader; tb: tBytes; Size: Integer): Boolean;
begin
  result := (Size >= sizeof(TMsgHeader));
  if (result) then
  begin
    move(tb[0], AMsgHeader, sizeof(TMsgHeader));
    result := TMsgHeader(AMsgHeader).ValidOpcode;
  end;
end;

procedure TgoMongoProtocol.Send(const adata: tBytes);
begin
  if IsConnected then
  begin
    FConnectionLock.Acquire;
    try
      if (FConnection <> nil) then
        FConnection.Send(adata);
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

function TgoMongoProtocol.SupportsOpMsg: Boolean;
begin
  result := True; // (FMaxWireVersion >= 6);
end;

function TgoMongoProtocol.TryGetReply(const ARequestId: Integer; out AReply: IgoMongoReply): Boolean;
begin
  FRepliesLock.Acquire;
  try
    result := FCompletedReplies.TryGetValue(ARequestId, AReply);
  finally
    FRepliesLock.Release;
  end;
end;

function TgoMongoProtocol.WaitForReply(const ARequestId: Integer): IgoMongoReply;
var
  Start, LastRecv: tStopWatch;
  ms: Int64;
begin
  { TODO : Handle per-request timeout as in findoptions.maxTimeMS }
  result := nil;
  Start := ThisMoment;
  ms := 0;
  while (ConnectionState = TgoConnectionState.Connected) and (not TryGetReply(ARequestId, result)) do
  begin
    if LastPartialReply(ARequestId, LastRecv) then // have partial reply ?
      ms := LastRecv.ElapsedMilliseconds // number of milliseconds "radiosilence" in large response
    else // no partial reply either
      ms := Start.ElapsedMilliseconds;

    if ms > FSettings.ReplyTimeout then
      Break;

    Sleep(5);
  end;

  if (result = nil) then
    TryGetReply(ARequestId, result);

  RemoveReply(ARequestId);

  if (result = nil) then
    Recover; // There could be trash in the input buffer, blocking the system
end;

procedure TgoMongoProtocol.Recover;
var
  MsgHeader: TMsgHeader;
begin
  { remove trash from the reception buffer }
  FRecvBufferLock.Enter;
  try
    if (FRecvSize > 0) then
    begin
      // if it begins with a valid response header, remove its statistics
      if HaveReplyMsgHeader(MsgHeader) then
        RemoveReply(MsgHeader.RequestID);
      // clear the buffer
      FRecvSize := 0;
    end;
  finally
    FRecvBufferLock.Leave;
  end;
end;

procedure TgoMongoProtocol.SocketRecv(const ABuffer: Pointer; const ASize: Integer);
var
  MongoReply: IgoMongoReply;
  ProcessedBytes, BytesLeft: Integer;
  MsgHeader: TMsgHeader;
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
      case TgoMongoMsgReply.ValidateMessage(FRecvBuffer, FRecvSize, ProcessedBytes, MongoReply) of

        tgoReplyValidationResult.rvrOK:
          begin
            FRepliesLock.Acquire;
            try
              { Remove the partial reply timestamp }
              FPartialReplies.Remove(MongoReply.ResponseTo);
              { Add the completed reply to the dictionary }
              FCompletedReplies.Add(MongoReply.ResponseTo, MongoReply);
            finally
              FRepliesLock.Release;
            end;

            { remove the processed bytes from the buffer }
            if (ProcessedBytes = FRecvSize) then
              FRecvSize := 0
            else
            begin
              BytesLeft := FRecvSize - ProcessedBytes;
              if BytesLeft > 0 then // should always be true
              begin
                move(FRecvBuffer[ProcessedBytes], FRecvBuffer[0], BytesLeft);
                FRecvSize := BytesLeft;
              end
              else
                FRecvSize := 0;
            end;
            { TODO : Faster wakeup of waiting thread ??? }
          end;

        tgoReplyValidationResult.rvrGrowing:
          begin
            // header opcode is valid but message still growing/incomplete
            // Update the partial reply timestamp
            if HaveReplyMsgHeader(MsgHeader) then
              UpdateReplyTimeout(MsgHeader.ResponseTo);
            Break;
          end;

        tgoReplyValidationResult.rvrOpcodeInvalid:
          begin
            // whatever is at the start of the buffer is not a valid header, discard everything
            FRecvSize := 0; // discard everything
            Break;
          end;

        tgoReplyValidationResult.rvrDataError, tgoReplyValidationResult.rvrChecksumInvalid, tgoReplyValidationResult.rvrCompressorError,
          tgoReplyValidationResult.rvrCompressorNotSupported:
          begin
            // There seems to be a valid header and there are enough bytes in the buffer, but the parser or checksum failed
            if HaveReplyMsgHeader(MsgHeader) then
              RemoveReply(MsgHeader.ResponseTo);
            FRecvSize := 0; // discard everything
            Break;
          end
      else
        Break;
      end; // case
    end;
  finally
    FRecvBufferLock.Leave;
  end;
end;

procedure TgoMongoProtocol.UpdateReplyTimeout(const ARequestId: Integer);
begin
  FRepliesLock.Acquire;
  try
    FPartialReplies.AddOrSetValue(ARequestId, ThisMoment);
  finally
    FRepliesLock.Release;
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

function TMsgHeader.Compressed: Boolean;
begin
  result := (self.OpCode = OP_COMPRESSED);
end;

function TMsgHeader.DataSize: Integer;
begin
  result := MessageLength - sizeof(self);
end;

function TMsgHeader.DataStart: Pointer;
begin
  result := Pointer(nativeuint(@self) + sizeof(self));
end;

function TOPCompressedHeader.CompressedDataSize: Integer;
begin
  result := Header.MessageLength - sizeof(self);
end;

function TOPCompressedHeader.DataStart: Pointer;
begin
  result := Pointer(nativeuint(@self) + sizeof(self));
end;

function TOPCompressedHeader.UnCompressedDataSize: Integer;
begin
  result := UncompressedSize;
end;

function TOPCompressedHeader.UnCompressedMessageSize: Integer;
begin
  result := UnCompressedDataSize + sizeof(Header);
end;

function TMsgHeader.ValidOpcode: Boolean;
begin
  { VERY basic format detection, but better than nothing }
  result := (self.OpCode = OP_MSG) or (self.OpCode = OP_COMPRESSED);
end;

{ TgoMongoMsgReply }

// Read data from a previously validated data buffer
procedure TgoMongoMsgReply.ReadData(const ABuffer: Pointer; const ASize: Integer);
var
  I, k: Integer;
  DocBuf: TArray<tBytes>;
  data: Pointer;
  StartOfData, Avail, SizeRead, NewSize: Integer;
  PayloadType: Byte;
  seqname: string;
begin
  fFirstDoc.SetNil;
  if (ASize >= sizeof(TOPMSGHeader)) then
  begin
    move(ABuffer^, FHeader, sizeof(FHeader));
    // read the header
    StartOfData := sizeof(FHeader);

    data := Pointer(nativeuint(ABuffer) + StartOfData);

    Avail := FHeader.Header.MessageLength - StartOfData;
    while tMsgPayload.DecodeSequence(False, data, Avail, SizeRead, PayloadType, seqname, DocBuf) = tgoPayloadDecodeResult.pdOK do
    begin
      case PayloadType of
        0:
          begin
            k := length(FPayload0);
            NewSize := k + length(DocBuf);
            SetLength(FPayload0, NewSize);
            for I := 0 to high(DocBuf) do
              FPayload0[k + I] := DocBuf[I];
          end;

        1:
          begin
            SetLength(FPayload1, length(FPayload1) + 1);
            FPayload1[high(FPayload1)].Name := seqname;
            FPayload1[high(FPayload1)].Docs := DocBuf;
          end;
      end;

      Avail := Avail - SizeRead;
      inc(intptr(data), SizeRead);
    end;
  end;
end;

// Validate a message in OP_MSG format

class function TgoMongoMsgReply.ValidateOPMessage(const ABuffer: tBytes; const ASize: Integer; var aSizeRead: Integer;
  out AReply: IgoMongoReply): tgoReplyValidationResult;
var
  DocBuf: TArray<tBytes>;
  data: Pointer;
  StartOfData, Avail: Integer;
  PayloadType: Byte;
  seqname: string;
  pHeader: POpMsgHeader;
  SizeRead, Type0Docs: Integer;
  AllBytesRead, HasChecksum: Boolean;

  function ChecksumOK: Boolean;
  begin
    { TODO : Implement checksum check, there's a tCRC32 at the end of the message }
    result := True;
  end;

begin
  result := tgoReplyValidationResult.rvrNoHeader; // Buffer does not contain enough bytes for a header
  AReply := nil;
  SizeRead := 0;
  Type0Docs := 0;
  try
    if (ASize >= sizeof(TOPMSGHeader)) then
    begin
      pHeader := @ABuffer[0];
      if pHeader.Header.ValidOpcode then
      begin
        if ASize >= pHeader.Header.MessageLength then
        begin
          StartOfData := sizeof(TOPMSGHeader); // data starts right after the header
          aSizeRead := StartOfData;
          data := @ABuffer[StartOfData];
          Avail := pHeader.Header.MessageLength - StartOfData;
          HasChecksum := TGoMongoMsgFlag.msgfChecksumPresent in pHeader.flagbits;

          if (not HasChecksum) or ChecksumOK() then // needs compiler switch $B-
          begin
            repeat
              case tMsgPayload.DecodeSequence(True, data, Avail, SizeRead, PayloadType, seqname, DocBuf) of
                tgoPayloadDecodeResult.pdOK:
                  begin
                    if PayloadType = 0 then
                      inc(Type0Docs);
                    Avail := Avail - SizeRead;
                    inc(intptr(data), SizeRead);
                    inc(aSizeRead, SizeRead);
                  end;

                tgoPayloadDecodeResult.pdEOF:
                  Break;

                tgoPayloadDecodeResult.pdInvalidPayloadType, tgoPayloadDecodeResult.pdBufferOverrun:
                  Exit(tgoReplyValidationResult.rvrDataError);

              end; // Case
            until False;

            // packet MUST have ONE document of payload type 0
            if Type0Docs <> 1 then
              Exit(tgoReplyValidationResult.rvrDataError);

            // Does amount of data parsed match the message length of the header ?
            if (HasChecksum) then
            begin
              AllBytesRead := (aSizeRead = pHeader.Header.MessageLength - sizeof(tCRC32));
              if AllBytesRead then
                inc(aSizeRead, sizeof(tCRC32)); // jump over checksum
            end // if checksum
            else
              AllBytesRead := (aSizeRead = pHeader.Header.MessageLength);
            if AllBytesRead then
            begin
              result := tgoReplyValidationResult.rvrOK;
              AReply := TgoMongoMsgReply.Create(@ABuffer[0], aSizeRead);
            end
            // Message decodes OK. All is well.
            else
              result := tgoReplyValidationResult.rvrDataError; // Header opcode OK, message could be complete, but decoding fails
          end // if checksum OK
          else
            result := tgoReplyValidationResult.rvrChecksumInvalid; // Header opcode OK, message could be complete, CRC fails
        end // if aSize big enough for data
        else
          result := tgoReplyValidationResult.rvrGrowing; // Header opcode OK, but message not complete yet
      end // if valid opcode
      else
        result := tgoReplyValidationResult.rvrOpcodeInvalid; // Invalid header, opcode unknown
    end // if enough bytes for a header
    else
      result := tgoReplyValidationResult.rvrNoHeader; // Buffer does not contain enough bytes for a header
  except
    // no exceptions allowed to exit
  end;
end;

class function TgoMongoMsgReply.ValidateMessage(const ABuffer: tBytes; const ASize: Integer; var aSizeRead: Integer;
  out AReply: IgoMongoReply): tgoReplyValidationResult;

var
  Source: POPCompressedHeader;
  Target: PMsgHeader;
  Unpacked: tBytes;
  // CompressedMessageLength, Uncompressedmessagelength, CompressedDataSize, datasize: Integer;

begin
  AReply := nil;
  aSizeRead := 0;
  result := tgoReplyValidationResult.rvrNoHeader;

  { Distinguish between compressed and uncompressed messages }

  if (ASize >= sizeof(TMsgHeader)) then
  begin
    Source := @ABuffer[0];
    if Source.Header.Compressed then // has a VALID op_compressed opcode
    begin
      if ASize < Source.Header.MessageLength then
        Exit(tgoReplyValidationResult.rvrGrowing);

      if Source.CompressorID > 2 then
        Exit(tgoReplyValidationResult.rvrCompressorNotSupported);

      SetLength(Unpacked, Source.UnCompressedMessageSize);

      // Prepare the "uncompressed" header
      Target := @Unpacked[0];
      Target.MessageLength := Source.UnCompressedMessageSize;
      Target.RequestID := Source.Header.RequestID;
      Target.ResponseTo := Source.Header.ResponseTo;
      Target.OpCode := OP_MSG;

      case Source.CompressorID of

        0: { noop }
          begin
            if not tNoopCompressor.Expand(Source.DataStart, Source.CompressedDataSize, Source.UnCompressedDataSize, Target.DataStart) then
              Exit(tgoReplyValidationResult.rvrCompressorError);
          end;

        1: { snappy }
          begin
            if not tSnappycompressor.Expand(Source.DataStart, Source.CompressedDataSize, Source.UnCompressedDataSize, Target.DataStart) then
              Exit(tgoReplyValidationResult.rvrCompressorError);
          end; // case

        2: { zlib }
          begin
            if not tZlibCompressor.Expand(Source.DataStart, Source.CompressedDataSize, Source.UnCompressedDataSize, Target.DataStart) then
              Exit(tgoReplyValidationResult.rvrCompressorError);
          end; // case
      end;

      result := ValidateOPMessage(Unpacked, Source.UnCompressedMessageSize, aSizeRead, AReply);
      if result = tgoReplyValidationResult.rvrOK then
        aSizeRead := Source.Header.MessageLength; // Bytes to discard!
    end // if compressed
    else
      result := ValidateOPMessage(ABuffer, ASize, aSizeRead, AReply); // uncompressed or TRASH
  end
  else
    result := tgoReplyValidationResult.rvrNoHeader; // not enough bytes for a header

end;

constructor TgoMongoMsgReply.Create(const ABuffer: Pointer; const ASize: Integer);
begin
  inherited Create;
  ReadData(ABuffer, ASize);
end;

function TgoMongoMsgReply._FirstDoc: TgoBsonDocument;
begin
  if fFirstDoc.IsNil then
  begin
    if length(FPayload0) > 0 then
      fFirstDoc := TgoBsonDocument.Load(FPayload0[0]);
  end;
  result := fFirstDoc;
end;

function TgoMongoMsgReply._GetPayload0: TArray<tBytes>;
begin
  result := FPayload0;
end;

function TgoMongoMsgReply._GetPayload1: TArray<tgoPayloadType1>;
begin
  result := FPayload1;
end;

function TgoMongoMsgReply._GetResponseTo: Integer;
begin
  result := FHeader.Header.ResponseTo;
end;

{ TgoMongoProtocolSettings }

initialization

end.
