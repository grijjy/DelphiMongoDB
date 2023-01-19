unit Snappy;

interface

uses sysutils;

{ ************************************************************************
  This unit is a heavily modified version of Roberto Della Pasqua's work,
  for the original code please see https://www.dellapasqua.com/snappy64/

  This unit currently only supports Windows, but the aim is to become
  platform independent, therefore it WILL compile on unsupported platforms,
  returning either "SNAPPY_NOT_IMPLEMENTED" or "False" on most calls.

  Please also see included file snappy_license.txt
  ************************************************************************ }

const
  SNAPPY_NOT_IMPLEMENTED = -1; // mine
  SNAPPY_OK = 0;
  SNAPPY_INVALID_INPUT = 1;
  SNAPPY_BUFFER_TOO_SMALL = 2;

{$IFDEF mswindows}
  Snappy_implemented = True; {$DEFINE snappy_implemented}
{$ELSE}
  Snappy_implemented = False;
{$ENDIF}
// only Win32 *.obj files pre-pend an underscore to all function names
{$IFDEF WIN32} _PU = '_'; {$DEFINE obj_underscore}{$ELSE} _PU = ''; {$ENDIF}

type
  Snappy_Status = Integer;   //Always 32 bit

  snappy_env = record
    hash_table: ^word;
    scratch: Pointer;
    scratch_output: Pointer;
  end;

  Psnappy_env = ^snappy_env;

function snappy_init_env(env: Psnappy_env): Snappy_Status; cdecl; //
{$IFDEF snappy_implemented}
external name _PU + 'snappy_init_env';
{$ENDIF}
procedure snappy_free_env(env: Psnappy_env); cdecl; //
{$IFDEF snappy_implemented}
external name _PU + 'snappy_free_env';
{$ENDIF}
function snappy_uncompress(compressed: Pchar; n: NativeUInt; uncompressed: Pchar): Snappy_Status; cdecl; //
{$IFDEF snappy_implemented}
external name _PU + 'snappy_uncompress';
{$ENDIF}
function snappy_compress(env: Psnappy_env; input: Pchar; input_length: NativeUInt; compressed: Pchar; compressed_length: PNativeUInt)
  : Snappy_Status; cdecl; //
{$IFDEF snappy_implemented}
external name _PU + 'snappy_compress';
{$ENDIF}
function snappy_max_compressed_length(source_len: NativeUInt): NativeUInt; cdecl; //
{$IFDEF snappy_implemented}
external name _PU + 'snappy_max_compressed_length';
{$ENDIF}
{ TODO : ATTENTION, newer versions of the Snappy C API appear to return a snappy_status
  instead of a boolean. This code has to be updated as soon as I can get my hands on
  newer object files. }

function snappy_uncompressed_length(buf: Pchar; len: NativeUInt; aresult: PNativeUInt): boolean; cdecl; //
{$IFDEF snappy_implemented}
external name _PU + 'snappy_uncompressed_length';
{$ENDIF}

implementation

{$REGION 'Stubs for platforms for which there is no Snappy support yet.'}
{$IFNDEF snappy_implemented}

function snappy_init_env(env: Psnappy_env): Snappy_Status; cdecl; //
begin
  result := SNAPPY_NOT_IMPLEMENTED;
end;

procedure snappy_free_env(env: Psnappy_env); cdecl; //
begin
end;

function snappy_uncompress(compressed: Pchar; n: NativeUInt; uncompressed: Pchar): Snappy_Status; cdecl; //
begin
  result := SNAPPY_NOT_IMPLEMENTED;
end;

function snappy_compress(env: Psnappy_env; input: Pchar; input_length: NativeUInt; compressed: Pchar; compressed_length: PNativeUInt)
  : Snappy_Status; cdecl; //
begin
  result := SNAPPY_NOT_IMPLEMENTED;
  compressed_length^ := 0;
end;

function snappy_uncompressed_length(buf: Pchar; len: NativeUInt; aresult: PNativeUInt): Snappy_Status; cdecl; //
begin
  result := SNAPPY_NOT_IMPLEMENTED;
  aresult^ := 0;
end;

function snappy_max_compressed_length(source_len: NativeUInt): NativeUInt; cdecl; //
begin
  result := 2 * source_len;
end;

{$ENDIF}
{$ENDREGION}
//
{$REGION 'Emulate C heap manager - object files need this'}
{ *******************************************************************************
  Implement some basic functions that the included object files need.
  ******************************************************************************* }

procedure _assert(__cond, __file: Pchar; __line: Integer); cdecl;
begin
  raise Exception.CreateFmt('Assertion failed: %s, file %s, line %d', [__cond, __file, __line]);
end;

{$IFDEF obj_underscore}

procedure _free(P: Pointer); cdecl;
begin
  FreeMem(P);
end;

function _malloc(size: Integer): Pointer; cdecl;
begin
  GetMem(result, size);
end;

function _memcmp(p1, p2: PByte; n: Integer): Integer; cdecl;
    // This function is never called.
var
  I: Cardinal;
begin
  I := 0;
  while I < n do
  begin
    if (p1^ <> p2^) then
      Exit(Ord(p1^) - Ord(p2^));
    Inc(p1);
    Inc(p2);
    Inc(I);
  end;
  result := 0;
end;

procedure _memcpy(dest, source: Pointer; count: Integer); cdecl;
begin
  Move(source^, dest^, count);
end;

function _memmove(dest, src: Pointer; n: Cardinal): Pointer; cdecl;
begin
  Move(src^, dest^, n);
  result := dest;
end;

procedure _memset(P: Pointer; B: Integer; count: Integer); cdecl;
begin
  FillChar(P^, count, B);
end;

{$ELSE}

procedure free(P: Pointer); cdecl;
begin
  FreeMem(P);
end;

function malloc(size: Integer): Pointer; cdecl;
begin
  GetMem(result, size);
end;

function memcmp(p1, p2: PByte; n: Integer): Integer; cdecl;
    // This function is never called.
var
  I: Integer;
begin
  I := 0;
  while I < n do
  begin
    if (p1^ <> p2^) then
      Exit(p1^ - p2^);
    Inc(p1);
    Inc(p2);
    Inc(I);
  end;
  result := 0;
end;

procedure memcpy(dest, source: Pointer; count: Integer); cdecl;
begin
  Move(source^, dest^, count);
end;

function memmove(dest, src: Pointer; n: Cardinal): Pointer; cdecl;
begin
  Move(src^, dest^, n);
  result := dest;
end;

procedure memset(P: Pointer; B: Integer; count: Integer); cdecl;
begin
  FillChar(P^, count, B);
end;

{$ENDIF obj_underscore}
{$ENDREGION}
//
{$REGION 'Object files for supported platforms'}
{ The object files are linked at this late stage so they "see" the local procedures above }
//
{$IFDEF WIN64}
{$L 'snappy.o'}
{$ENDIF}
//
{$IFDEF WIN32}
{$L 'snappy.obj'}
{$ENDIF}
//

{$ENDREGION}
//

end.
