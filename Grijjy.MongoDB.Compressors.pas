unit Grijjy.MongoDB.Compressors;

interface

uses system.SysUtils, system.ZLib, Snappy;

function Snappy_Implemented: Boolean;

const
  ZLIB_Implemented = True;

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
  tSnappyCompressor = class
    class function Compress(adata: Pointer; aUncompressedSize: Integer; var aCompressedSize: Integer; var Output: tBytes): Boolean;
    class function Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; OutputBuffer: Pointer): Boolean;
  end;

implementation

{$HINTS OFF}

function Snappy_Implemented: Boolean;
begin
  result := Snappy.Snappy_Implemented;
end;

class function tSnappyCompressor.Compress(adata: Pointer; aUncompressedSize: Integer; var aCompressedSize: Integer;
  var Output: tBytes): Boolean;
var
  OutSize: Integer;
  OutBuffer: Pointer;
var
  OutLen: NativeUInt;
  Status: Snappy_Status;
begin
  result := false;
  if Snappy_Implemented then
  begin
    if aUncompressedSize <= 0 then
      Exit
    else
    begin
      outlen := snappy_max_compressed_length(aUncompressedSize);
      setlength(Output, outlen);
      status := snappy_compress(adata, aUncompressedSize, @Output[0], outlen);
      if status = Snappy_OK then
      begin
        aCompressedSize := outlen;
        setlength(Output, aCompressedSize); // truncate
        result := True;
      end;
    end;
  end;
end;

class function tSnappyCompressor.Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; OutputBuffer: Pointer): Boolean;
var
  ExpandedSize: NativeUInt;
  tempbuffer: tBytes;
  status: Snappy_Status;
begin
  result := false;
  if Snappy_Implemented then
  begin
    if aCompressedSize <= 0 then
      Exit(false)
    else
    begin
      status := snappy_uncompressed_length(adata, aCompressedSize, ExpandedSize);
      result := (status = Snappy_OK);
      if result then
      begin
        result := (ExpandedSize = aUncompressedSize);
        if result then
        begin
          setlength(tempbuffer, ExpandedSize);
          status := snappy_uncompress(adata, aCompressedSize, @tempbuffer[0], ExpandedSize);
          result := (status = Snappy_OK);
          if result then
            Move(tempbuffer[0], OutputBuffer^, ExpandedSize);
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
  result := True;
end;

class function tNoopCompressor.Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; OutputBuffer: Pointer): Boolean;
begin
  Move(adata^, OutputBuffer^, aCompressedSize);
  result := True;
end;

class function tZlibCompressor.Compress(adata: Pointer; aUncompressedSize: Integer; var aCompressedSize: Integer;
  var Output: tBytes): Boolean;
var
  OutSize: Integer;
  OutBuffer: Pointer;
begin
  result := false;
  if aUncompressedSize <= 0 then
    Exit
  else
  begin
    result := True;
    zcompress(adata, aUncompressedSize, OutBuffer, aCompressedSize, clDefault);
    if aCompressedSize > 0 then
    begin
      setlength(Output, aCompressedSize);
      Move(OutBuffer^, Output[0], aCompressedSize);
      FreeMem(OutBuffer);
    end;
  end;
end;

class function tZlibCompressor.Expand(adata: Pointer; aCompressedSize, aUncompressedSize: Integer; OutputBuffer: Pointer): Boolean;
var
  ExpandedSize: Integer;
  tempbuffer: Pointer;
begin
  result := false;
  if aCompressedSize <= 0 then
    Exit(false)
  else
  begin
    ZDecompress(adata, aCompressedSize, tempbuffer, ExpandedSize, 0);
    if ExpandedSize = aUncompressedSize then
    begin
      Move(tempbuffer^, OutputBuffer^, aUncompressedSize);
      FreeMem(tempbuffer);
      result := True;
    end;
  end;
end;

end.
