program snappytest;

{$APPTYPE CONSOLE}
{$R *.res}

uses
  System.SysUtils,
  Snappy in 'Snappy.pas';

procedure test;
var TestData: Ansistring;
  InputBytes, CompressedSize, BufSize, ExpandedSize: Nativeuint;
  status: snappy_status;
  CompressedBuf, ExpandedBuf: pansichar;

begin
   if not Snappy_Implemented then
     raise exception.Create (format('Please copy file %s to directory %s before testing this program.',[SnappyDLL, extractfiledir(paramstr(0))]));


  TestData := 'bibit pauper et egrotus, bibit exul et ignotus, bibit puer, bibit canus,' +
    'bibit presul et decanus, bibit soror, bibit frater, bibit anus, bibit mater,' +
    'bibit ista, bibit ille, bibunt centum, bibunt mille.';

  InputBytes := length(TestData);

  // Allocate an output buffer for the compressed data
  BufSize := snappy_max_compressed_length(InputBytes);
  getmem(CompressedBuf, BufSize);

  // Compress the data. CompressedSize must initially be set to bufsize.
  CompressedSize := BufSize;
  status := snappy_compress(@TestData[1], length(TestData), CompressedBuf, CompressedSize);
  Assert(status = SNAPPY_OK);

  // Check if it would expand correctly to the same size
  status := snappy_uncompressed_length(CompressedBuf, CompressedSize, ExpandedSize);
  Assert(status = SNAPPY_OK);
  Assert(ExpandedSize = InputBytes);

  // Allocate a buffer for expansion
  getmem(ExpandedBuf, ExpandedSize);

  // Expand
  status := snappy_uncompress(CompressedBuf, CompressedSize, ExpandedBuf, ExpandedSize);
  Assert(status = SNAPPY_OK);
  Assert(ExpandedSize = InputBytes);

  // Cleanup
  freemem(ExpandedBuf);
  freemem(CompressedBuf);
end;

begin
  try
    test;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;

end.
