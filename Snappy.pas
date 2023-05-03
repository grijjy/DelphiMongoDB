unit Snappy;

interface

uses System.SysUtils {$IFDEF MSWINDOWS}, Winapi.Windows{$ENDIF};

{ ************************************************************************
Delphi Port for Google's Snappy compression, by Arthur Hoornweg.
It uses DLL versions of Google's snappy compressor, 
created by MongoDB inc.
----------------------------------------------------------------------------
This unit borrows these dll's (Snappy32.dll, snappy64.dll,libsnappy64.so,libsnappy64.dylib)
from Mongodb's Nuget Package "MongoDB.Driver.Core" version 2.15.1
(https://www.nuget.org/packages/MongoDB.Driver)
---------------------------------------------------------------------------
 Currently only the Windows platform is tested and verified to work.
---------------------------------------------------------------------------
How to update the dll's:  
-Start Visual Studio
-Create a new C# Console App (.NET Framework) and target .NET Framework 4.7.2
-Right click the Solution and choose Manage NuGet Packages for Solution...
-Browse for the MongoDB.Driver.Core and install version 2.15.1
-Observe that the following new files are added to the solution and included in the VCS:
-Core/Compression/Snappy/lib/win/snappy32.dll
-Core/Compression/Snappy/lib/win/snappy64.dll     etcetera.


************************************************************************ }

type
  Snappy_Status = Integer;

const
  SNAPPY_NOT_IMPLEMENTED = -1; // mine
  SNAPPY_OK = 0;
  SNAPPY_INVALID_INPUT = 1;
  SNAPPY_BUFFER_TOO_SMALL = 2;

const
{$IFDEF LINUX64}
  SnappyDLL = 'libsnappy64.so';
{$ELSE}
{$IFDEF MACOS64}
  SnappyDLL = 'libsnappy64.dylib';
{$ELSE}
{$IFDEF MSWINDOWS}
{$IFDEF CPU64BITS}
  SnappyDLL = 'snappy64.dll';
{$ELSE}
  SnappyDLL = 'snappy32.dll';
{$ENDIF}
{$ELSE}
SnappyDLL = ''
{$ENDIF}
{$ENDIF}
{$ENDIF}
{ Snappy_Implemented
 Returns TRUE if this unit can do snappy compression and decompression }
function Snappy_Implemented: Boolean;

{ snappy_compress
  Input: Points to the data to be compressed.
  InputLength: the length of the input data.
  Compressed: points to the output buffer.
  CompressedLength: Must be initialized with the size of the output buffer.
  !!! CompressedLength will be updated with the actual compressed data size }
function snappy_compress(Input: pAnsiChar; Inputlength: NativeUInt; Compressed: pAnsiChar; var CompressedLength: NativeUInt): Snappy_Status;

{ snappy_uncompress
  compressed: Points to the data to be expanded.
  compressedLength: the length of the compressed data.
  Output: points to the output buffer.
  OutputLength: Must be initialized with the size of the output buffer.
  !!! Outputlength will be updated with the actual expanded data size }
function snappy_uncompress(Compressed: pAnsiChar; CompressedLength: NativeUInt; Output: pAnsiChar; var Outputlength: NativeUInt)
  : Snappy_Status;

{ snappy_max_compressed_length
 calculates how big the compressed data can be in the worst case }
function snappy_max_compressed_length(sourcelen: NativeUInt): NativeUInt;

{ snappy_uncompressed_length
 calculates how big the uncompressed data will be }
function snappy_uncompressed_length(Compressed: pAnsiChar; CompressedLength: NativeUInt; out aResult: NativeUInt): Snappy_Status;

{ snappy_validate_compressed_buffer
 Checks if compressed data is valid }
function snappy_validate_compressed_buffer(Compressed: pAnsiChar; CompressedLength: NativeUInt): Snappy_Status;

implementation

const
  ModuleNotInitialized = 0;
  ModuleError = Hmodule(-1);

var
  hdll: Hmodule = ModuleNotInitialized;
  obj: tObject;

type
  tsnappy_max_compressed_length = function(sourcelen: NativeUInt): NativeUInt; cdecl;
  tsnappy_uncompress = function(Compressed: pAnsiChar; CompressedLength: NativeUInt; Output: pAnsiChar; var Outputlength: NativeUInt)
    : Snappy_Status; cdecl;
  tsnappy_compress = function(Input: pAnsiChar; Input_length: NativeUInt; Compressed: pAnsiChar; var CompressedLength: NativeUInt)
    : Snappy_Status; cdecl;
  tsnappy_uncompressed_length = function(Buf: pAnsiChar; CompressedLength: NativeUInt; out aResult: NativeUInt): Snappy_Status; cdecl;
  tsnappy_validate_compressed_buffer = function(Compressed: pAnsiChar; CompressedLength: NativeUInt): Snappy_Status; cdecl;

var
  _snappy_uncompress: tsnappy_uncompress = nil;
  _snappy_compress: tsnappy_compress = nil;
  _snappy_max_compressed_length: tsnappy_max_compressed_length = nil;
  _snappy_uncompressed_length: tsnappy_uncompressed_length = nil;
  _snappy_validate_compressed_buffer: tsnappy_validate_compressed_buffer = nil;

function HandleValid: Boolean;
begin
  Result := ((hdll <> ModuleNotInitialized) and (hdll <> ModuleError));
end;

function Snappy_Implemented: Boolean;
var HM: Hmodule;
begin
  if HandleValid() then
    Exit(True)
  else if (hdll = ModuleError) or (SnappyDLL = '') then
    Exit(False)
  else
  begin
    { hdll=ModuleNotInitialized --> try to load the DLL.
     Avoid race condition:another thread might attempt
     to do the same thing. }
    tmonitor.Enter(obj);
    try
      { Check if another thread has tried the same thing.
      3 possibilities:
      Hdll=ModuleError  --> DLL could not be loaded
      Hdll=Valid handle --> DLL already loaded
      hdll=ModuleNotInitialized --> Try to load it now }
      if HandleValid() then
        Exit(True)
      else if (hdll = ModuleError) then
        Exit(False)
      else
      begin // dll must be loaded.
        HM := loadlibrary(SnappyDLL);
        if (HM = 0) then
        begin
          // DLL could not be loaded, set permanent error
          hdll := ModuleError; // atomic
          Exit(False);
        end // if loading failed
        else
        begin
        // loading succeeded
          _snappy_uncompress := GetProcAddress(HM, 'snappy_uncompress');
          _snappy_compress := GetProcAddress(HM, 'snappy_compress');
          _snappy_max_compressed_length := GetProcAddress(HM, 'snappy_max_compressed_length');
          _snappy_uncompressed_length := GetProcAddress(HM, 'snappy_uncompressed_length');
          _snappy_validate_compressed_buffer := GetProcAddress(HM, 'snappy_validate_compressed_buffer');
          hdll := HM; // atomic
          Exit(True); // success
        end; // if loading succeeded
      end;
    finally
      tmonitor.Exit(obj);
    end;
  end;
end;

function snappy_uncompress(Compressed: pAnsiChar; CompressedLength: NativeUInt; Output: pAnsiChar; var Outputlength: NativeUInt)
  : Snappy_Status;
begin
  if Snappy_Implemented() and assigned(_snappy_uncompress) then
    Result := _snappy_uncompress(Compressed, CompressedLength, Output, Outputlength)
  else
    Result := SNAPPY_NOT_IMPLEMENTED;
end;

function snappy_compress(Input: pAnsiChar; Inputlength: NativeUInt; Compressed: pAnsiChar; var CompressedLength: NativeUInt): Snappy_Status;
begin
  if Snappy_Implemented and assigned(_snappy_compress) then
    Result := _snappy_compress(Input, Inputlength, Compressed, CompressedLength)
  else
    Result := SNAPPY_NOT_IMPLEMENTED;
end;

function snappy_max_compressed_length(sourcelen: NativeUInt): NativeUInt;
begin
  if Snappy_Implemented and assigned(_snappy_max_compressed_length) then
    Result := _snappy_max_compressed_length(sourcelen)
  else
    Result := 0;
end;

function snappy_uncompressed_length(Compressed: pAnsiChar; CompressedLength: NativeUInt; out aResult: NativeUInt): Snappy_Status;
begin
  if Snappy_Implemented and assigned(_snappy_uncompressed_length) then
    Result := _snappy_uncompressed_length(Compressed, CompressedLength, aResult)
  else
    Result := SNAPPY_NOT_IMPLEMENTED;
end;

function snappy_validate_compressed_buffer(Compressed: pAnsiChar; CompressedLength: NativeUInt): Snappy_Status;
begin
  if Snappy_Implemented and assigned(_snappy_validate_compressed_buffer) then
    Result := _snappy_validate_compressed_buffer(Compressed, CompressedLength)
  else
    Result := SNAPPY_NOT_IMPLEMENTED;
end;

initialization

obj := tObject.Create;

finalization

obj.Free;

end.
