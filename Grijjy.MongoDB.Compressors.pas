unit Grijjy.MongoDB.Compressors;

interface

uses system.SysUtils, system.ZLib, Snappy;


Const Snappy_Implemented=Snappy.Snappy_implemented;
      ZLIB_Implemented=True;


type
  tNoopCompressor = class
    class function Compress(adata: Pointer; aUncompressedSize: Integer; var aCompressedSize: Integer; var Output: tBytes): Boolean;
    class function Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; OutputBuffer: Pointer): Boolean;
  end;

  tZlibCompressor = class
    class function Compress(adata: Pointer; aUncompressedSize: Integer; var aCompressedSize: Integer; var Output: tBytes): Boolean;
    class function Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; OutputBuffer: Pointer): Boolean;
  end;

  // currently support Snappy only under Windows
type
  tSnappyCompressor = class
    class function Compress(adata: Pointer; aUncompressedSize: Integer; var aCompressedSize: Integer; var Output: tBytes): Boolean;
    class function Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; OutputBuffer: Pointer): Boolean;
  end;

implementation

{$HINTS OFF}

class function tSnappyCompressor.Compress(adata: Pointer; aUncompressedSize: Integer; var aCompressedSize: Integer;
  var Output: tBytes): Boolean;
var
  outsize: Integer;
  outbuffer: Pointer;
var
  env: snappy_env;
  outlen: NativeUInt;
  status: Snappy_Status;
begin
  result := false;
  if Snappy_Implemented then
  begin
    if aUncompressedSize <= 0 then
      Exit
    else
    begin
      status := snappy_init_env(@env);
      if status = Snappy_OK then
      begin
        setlength(Output, snappy_max_compressed_length(aUncompressedSize));
        snappy_compress(@env, adata, aUncompressedSize, @Output[0], @outlen);
        snappy_free_env(@env);
        aCompressedSize := outlen;
        setlength(Output, aCompressedSize);
        result := true;
      end;
    end;
  end;
end;

class function tSnappyCompressor.Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; OutputBuffer: Pointer): Boolean;
var
  expandedsize, temp: NativeUInt;
  tempbuffer: tBytes;
begin
  result := false;
  if Snappy_Implemented then
  begin
    if aCompressedSize <= 0 then
      Exit(false)
    else
    begin
      result:= snappy_uncompressed_length(adata, aCompressedSize, @expandedsize);
      if result then
      begin
        result := (expandedsize = aUncompressedSize);
        if result then
        begin
          setlength(tempbuffer, expandedsize);
          temp := snappy_uncompress(adata, aCompressedSize, @tempbuffer[0]);
          Move(tempbuffer[0], OutputBuffer^, expandedsize);
        end;
      end;
    end;
  end;
end;

class function tNoopCompressor.Compress(adata: Pointer; aUncompressedSize: Integer; var aCompressedSize: Integer;
  var Output: tBytes): Boolean;
begin
  aCompressedSize := aUncompressedSize;
  setlength(Output, aUncompressedSize);
  Move(adata^, Output[0], aUncompressedSize);
  result := true;
end;

class function tNoopCompressor.Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; OutputBuffer: Pointer): Boolean;
begin
  Move(adata^, OutputBuffer^, aCompressedSize);
  result := true;
end;

class function tZlibCompressor.Compress(adata: Pointer; aUncompressedSize: Integer; var aCompressedSize: Integer;
  var Output: tBytes): Boolean;
var
  outsize: Integer;
  outbuffer: Pointer;
begin
  result := false;
  if aUncompressedSize <= 0 then
    Exit
  else
  begin
    result := true;
    zcompress(adata, aUncompressedSize, outbuffer, aCompressedSize, clDefault);
    if aCompressedSize > 0 then
    begin
      setlength(Output, aCompressedSize);
      Move(outbuffer^, Output[0], aCompressedSize);
      FreeMem(outbuffer);
    end;
  end;
end;

class function tZlibCompressor.Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; OutputBuffer: Pointer): Boolean;
var
  expandedsize: Integer;
  tempbuffer: Pointer;
begin
  result := false;
  if aCompressedSize <= 0 then
    Exit(false)
  else
  begin
    ZDecompress(adata, aCompressedSize, tempbuffer, expandedsize, 0);
    if expandedsize = aUncompressedSize then
    begin
      Move(tempbuffer^, OutputBuffer^, aUncompressedSize);
      FreeMem(tempbuffer);
      result := true;
    end;
  end;
end;

end.
