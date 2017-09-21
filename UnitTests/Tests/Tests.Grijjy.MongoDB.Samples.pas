unit Tests.Grijjy.MongoDB.Samples;

interface

uses
  System.Generics.Collections,
  DUnitX.TestFramework,
  Grijjy.Bson,
  Grijjy.MongoDB,
  Grijjy.MongoDB.Queries;

type
  TTestDocumentationSamples = class
  private
    FClient: IgoMongoClient;
    FDatabase: IgoMongoDatabase;
    FCollection: IgoMongoCollection;
  private
    procedure RemoveIds(const ADocuments: TArray<TgoBsonDocument>);
    function ParseMultiple(const AJsons: array of String): TArray<TgoBsonDocument>;
    procedure CheckEquals(const AExpected, AActual: TArray<TgoBsonDocument>); overload;
  public
    [Setup] procedure Setup;
    [TearDown] procedure Teardown;
  public
    [Test] procedure Example01;
    [Test] procedure Example03;
  end;

type
  TPrimerFixture = class
  private class var
    FClient: IgoMongoClient;
    FDatabase: IgoMongoDatabase;
    FDataset: TList<TgoBsonDocument>;
    FReloadCollection: Boolean;
  private
    procedure LoadDataSetFromResource;
    procedure LoadCollection;
  protected
    procedure AltersCollection;
    procedure RemoveId(const ADocument: TgoBsonDocument);
    procedure CheckEquals(const AExpected, AActual: TgoBsonValue); overload;

    class property Database: IgoMongoDatabase read FDatabase;
  public
    class constructor Create;
    class destructor Destroy;
  public
    [Setup] procedure Setup;
    [SetupFixture] procedure SetupFixture;
  public
  end;

type
  TTestQueryPrimer = class(TPrimerFixture)
  public
    [Test] procedure QueryAll;
    [Test] procedure LogicalAnd;
    [Test] procedure LogicalOr;
    [Test] procedure QueryTopLevelField;
    [Test] procedure QueryEmbeddedDocument;
    [Test] procedure QueryFieldInArray;
    [Test] procedure GreaterThan;
    [Test] procedure LessThan;
    [Test] procedure Sort;
  end;

type
  TTestInsertPrimer = class(TPrimerFixture)
  public
    [Test] procedure InsertADocument;
  end;

type
  TTestRemovePrimer = class(TPrimerFixture)
  public
    [Test] procedure RemoveMatchingDocuments;
    [Test] procedure RemoveAllDocuments;
    [Test] procedure DropCollection;
  end;

type
  TTestUpdatePrimer = class(TPrimerFixture)
  public
    [Test] procedure UpdateTopLevelFields;
    [Test] procedure UpdateEmbeddedField;
    [Test] procedure UpdateMultipleDocuments;
  end;

implementation

uses
  System.Types,
  System.Classes,
  System.SysUtils,
  System.DateUtils,
  System.Zip,
  Tests.Grijjy.MongoDB.Settings;

{$R Resources.res}

{ TTestDocumentationSamples }

procedure TTestDocumentationSamples.CheckEquals(const AExpected,
  AActual: TArray<TgoBsonDocument>);
var
  I: Integer;
  Exp, Act: TArray<String>;
begin
  Assert.AreEqual(Length(AExpected), Length(AActual));
  SetLength(Exp, Length(AExpected));
  SetLength(Act, Length(AActual));
  for I := 0 to Length(AExpected) - 1 do
  begin
    Exp[I] := AExpected[I].ToJson;
    Act[I] := AActual[I].ToJson;
  end;

  TArray.Sort<String>(Exp);
  TArray.Sort<String>(Act);

  for I := 0 to Length(Exp) - 1 do
    Assert.AreEqual(Exp[I], Act[I]);
end;

procedure TTestDocumentationSamples.Example01;
var
  Doc: TgoBsonDocument;
  Result: TArray<TgoBsonDocument>;
begin
  Doc := TgoBsonDocument.Create()
    .Add('item', 'canvas')
    .Add('qty', 100)
    .Add('tags', TgoBsonArray.Create(['cotton']))
    .Add('size', TgoBsonDocument.Create()
                 .Add('h', 28)
                 .Add('w', 35.5)
                 .Add('uom', 'cm'));
  Assert.IsTrue(FCollection.InsertOne(Doc));

  Result := FCollection.Find.ToArray;
  RemoveIds(Result);
  CheckEquals(ParseMultiple(
    ['{ item: "canvas", qty: 100, tags: ["cotton"], size: { h: 28, w: 35.5, uom: "cm" } }']),
    Result);
end;

procedure TTestDocumentationSamples.Example03;
var
  Docs, Result: TArray<TgoBsonDocument>;
begin
  Docs := TArray<TgoBsonDocument>.Create(
    TgoBsonDocument.Create()
      .Add('item', 'journal')
      .Add('qty', 25)
      .Add('tags', TgoBsonArray.Create(['blank', 'red']))
      .Add('size', TgoBsonDocument.Create()
                   .Add('h', 14)
                   .Add('w', 21)
                   .Add('uom', 'cm')),

    TgoBsonDocument.Create()
      .Add('item', 'mat')
      .Add('qty', 85)
      .Add('tags', TgoBsonArray.Create(['gray']))
      .Add('size', TgoBsonDocument.Create()
                   .Add('h', 27.9)
                   .Add('w', 35.5)
                   .Add('uom', 'cm')),

    TgoBsonDocument.Create()
      .Add('item', 'mousepad')
      .Add('qty', 25)
      .Add('tags', TgoBsonArray.Create(['gel', 'blue']))
      .Add('size', TgoBsonDocument.Create()
                   .Add('h', 19)
                   .Add('w', 22.85)
                   .Add('uom', 'cm')));
  Assert.AreEqual(3, FCollection.InsertMany(Docs));

  Result := FCollection.Find.ToArray;
  RemoveIds(Result);
  CheckEquals(ParseMultiple(
    ['{ item: "journal", qty: 25, tags: ["blank", "red"], size: { h: 14, w: 21, uom: "cm" } }',
     '{ item: "mat", qty: 85, tags: ["gray"], size: { h: 27.9, w: 35.5, uom: "cm" } }',
     '{ item: "mousepad", qty: 25, tags: ["gel", "blue"], size: { h: 19, w: 22.85, uom: "cm" } }']),
    Result);
end;

function TTestDocumentationSamples.ParseMultiple(
  const AJsons: array of String): TArray<TgoBsonDocument>;
var
  I: Integer;
begin
  SetLength(Result, Length(AJsons));
  for I := 0 to Length(AJsons) - 1 do
    Result[I] := TgoBsonDocument.Parse(AJsons[I]);
end;

procedure TTestDocumentationSamples.RemoveIds(
  const ADocuments: TArray<TgoBsonDocument>);
var
  Doc: TgoBsonDocument;
begin
  for Doc in ADocuments do
    Doc.Remove('_id');
end;

procedure TTestDocumentationSamples.Setup;
begin
  FClient := TgoMongoClient.Create(TEST_SERVER_HOST, TEST_SERVER_PORT);
  FDatabase := FClient.GetDatabase('test');
  FCollection := FDatabase.GetCollection('inventory');
  FDatabase.DropCollection('inventory');
end;

procedure TTestDocumentationSamples.Teardown;
begin
  FCollection := nil;
  FDatabase := nil;
  FClient := nil;
end;

{ TPrimerFixture }

procedure TPrimerFixture.AltersCollection;
begin
  FReloadCollection := True;
end;

procedure TPrimerFixture.CheckEquals(const AExpected, AActual: TgoBsonValue);
begin
  Assert.AreEqual(AExpected.IsNil, AActual.IsNil);
  if (AExpected.IsNil) then
    Exit;

  Assert.AreEqual(AExpected.ToJson, AActual.ToJson);
end;

class constructor TPrimerFixture.Create;
begin
  FDataset := TList<TgoBsonDocument>.Create;
  FReloadCollection := True;
end;

class destructor TPrimerFixture.Destroy;
begin
  FreeAndNil(FDataset);
end;

procedure TPrimerFixture.LoadCollection;
var
  Collection: IgoMongoCollection;
begin
  FDatabase.DropCollection('restaurants');
  Collection := FDatabase.GetCollection('restaurants');
  Assert.AreEqual(25359, Collection.InsertMany(FDataset));
end;

procedure TPrimerFixture.LoadDataSetFromResource;
var
  Stream: TStream;
  ZipFile: TZipFile;
  Bytes: TBytes;
  Reader: TStreamReader;
  Line: String;
  Doc: TgoBsonDocument;
begin
  ZipFile := nil;
  Stream := TResourceStream.Create(HInstance, 'DATASET', RT_RCDATA);
  try
    ZipFile := TZipFile.Create;
    ZipFile.Open(Stream, zmRead);
    ZipFile.Read('dataset.json', Bytes);
  finally
    ZipFile.Free;
    Stream.Free;
  end;

  Reader := nil;
  Stream := TBytesStream.Create(Bytes);
  try
    Reader := TStreamReader.Create(Stream, TEncoding.UTF8);
    while (not Reader.EndOfStream) do
    begin
      Line := Reader.ReadLine;
      Doc := TgoBsonDocument.Parse(Line);
      FDataset.Add(Doc);
    end;
  finally
    Reader.Free;
    Stream.Free;
  end;
end;

procedure TPrimerFixture.RemoveId(const ADocument: TgoBsonDocument);
begin
  ADocument.Remove('_id');
end;

procedure TPrimerFixture.Setup;
begin
  if (FReloadCollection) then
  begin
    LoadCollection;
    FReloadCollection := False;
  end;
end;

procedure TPrimerFixture.SetupFixture;
begin
  if (FClient = nil) then
  begin
    FClient := TgoMongoClient.Create(TEST_SERVER_HOST, TEST_SERVER_PORT);
    FDatabase := FClient.GetDatabase('test');
    LoadDataSetFromResource;
    FReloadCollection := True;
  end;
end;

{ TTestQueryPrimer }

procedure TTestQueryPrimer.GreaterThan;
var
  Collection: IgoMongoCollection;
  Count: Integer;
  Doc: TgoBsonDocument;
begin
  Collection := Database.GetCollection('restaurants');
  Count := 0;

  for Doc in Collection.Find(TgoMongoFilter.Gt('grades.score', 30)) do
    Inc(Count);

  Assert.AreEqual(1959, Count);
end;

procedure TTestQueryPrimer.LessThan;
var
  Collection: IgoMongoCollection;
  Count: Integer;
  Doc: TgoBsonDocument;
begin
  Collection := Database.GetCollection('restaurants');
  Count := 0;

  for Doc in Collection.Find(TgoMongoFilter.Lt('grades.score', 10)) do
    Inc(Count);

  Assert.AreEqual(19065, Count);
end;

procedure TTestQueryPrimer.LogicalAnd;
var
  Collection: IgoMongoCollection;
  Count: Integer;
  Doc: TgoBsonDocument;
begin
  Collection := Database.GetCollection('restaurants');
  Count := 0;

  for Doc in Collection.Find(
    TgoMongoFilter.Eq('cuisine', 'Italian') and
    TgoMongoFilter.Eq('address.zipcode', '10075')
  ) do
    Inc(Count);

  Assert.AreEqual(15, Count);
end;

procedure TTestQueryPrimer.LogicalOr;
var
  Collection: IgoMongoCollection;
  Count: Integer;
  Doc: TgoBsonDocument;
begin
  Collection := Database.GetCollection('restaurants');
  Count := 0;

  for Doc in Collection.Find(
    TgoMongoFilter.Eq('cuisine', 'Italian') or
    TgoMongoFilter.Eq('address.zipcode', '10075')
  ) do
    Inc(Count);

  Assert.AreEqual(1153, Count);
end;

procedure TTestQueryPrimer.QueryAll;
var
  Collection: IgoMongoCollection;
  Count: Integer;
  Doc: TgoBsonDocument;
begin
  Collection := Database.GetCollection('restaurants');
  Count := 0;

  for Doc in Collection.Find() do
    Inc(Count);

  Assert.AreEqual(25359, Count);
end;

procedure TTestQueryPrimer.QueryEmbeddedDocument;
var
  Collection: IgoMongoCollection;
  Count: Integer;
  Doc: TgoBsonDocument;
begin
  Collection := Database.GetCollection('restaurants');
  Count := 0;

  for Doc in Collection.Find(TgoMongoFilter.Eq('address.zipcode', '10075')) do
    Inc(Count);

  Assert.AreEqual(99, Count);
end;

procedure TTestQueryPrimer.QueryFieldInArray;
var
  Collection: IgoMongoCollection;
  Count: Integer;
  Doc: TgoBsonDocument;
begin
  Collection := Database.GetCollection('restaurants');
  Count := 0;

  for Doc in Collection.Find(TgoMongoFilter.Eq('grades.grade', 'B')) do
    Inc(Count);

  Assert.AreEqual(8280, Count);
end;

procedure TTestQueryPrimer.QueryTopLevelField;
var
  Collection: IgoMongoCollection;
  Count: Integer;
  Doc: TgoBsonDocument;
begin
  Collection := Database.GetCollection('restaurants');
  Count := 0;

  for Doc in Collection.Find(TgoMongoFilter.Eq('borough', 'Manhattan')) do
    Inc(Count);

  Assert.AreEqual(10259, Count);
end;

procedure TTestQueryPrimer.Sort;
var
  Collection: IgoMongoCollection;
  Doc, Address: TgoBsonDocument;
  Count: Integer;
  Borough, ZipCode, PrevBorough, PrevZipCode: String;
begin
  Collection := Database.GetCollection('restaurants');

  PrevBorough := '';
  PrevZipCode := '';
  Count := 0;
  for Doc in Collection.Find(TgoMongoFilter.Empty,
    TgoMongoSort.Ascending('borough') +
    TgoMongoSort.Ascending('address.zipcode')) do
  begin
    Borough := Doc['borough'];
    Assert.IsTrue(Borough >= PrevBorough);
    if (Borough = PrevBorough) then
    begin
      Address := Doc['address'].AsBsonDocument;
      ZipCode := Address['zipcode'];
      Assert.IsTrue(ZipCode >= PrevZipCode);
      PrevZipCode := ZipCode;
    end
    else
      PrevZipCode := '';

    PrevBorough := Borough;
    Inc(Count);
  end;

  Assert.IsTrue(PrevBorough <> '');
  Assert.AreEqual(25359, Count);
end;

{ TTestInsertPrimer }

procedure TTestInsertPrimer.InsertADocument;
var
  Doc, Result: TgoBsonDocument;
  Collection: IgoMongoCollection;
begin
  AltersCollection;

  Doc := TgoBsonDocument.Create()
    .Add('address', TgoBsonDocument.Create()
                      .Add('street', '2 Avenue')
                      .Add('zipcode', '10075')
                      .Add('building', '1480')
                      .Add('coord', TgoBsonArray.Create([73.9557413, 40.7720266])))
    .Add('borough', 'Manhattan')
    .Add('cuisine', 'Italian')
    .Add('grades', TgoBsonArray.Create([
      TgoBsonDocument.Create()
        .Add('date', EncodeDateTime(2014, 10, 1, 0, 0, 0, 0))
        .Add('grade', 'A')
        .Add('score', 11),

      TgoBsonDocument.Create()
        .Add('date', EncodeDateTime(2014, 1, 6, 0, 0, 0, 0))
        .Add('grade', 'B')
        .Add('score', 17)]))
    .Add('name', 'Vella')
    .Add('restaurant_id', '941704620');

  Collection := Database.GetCollection('restaurants');
  Assert.IsTrue(Collection.InsertOne(Doc));

  Result := Collection.FindOne(TgoMongoFilter.Eq('restaurant_id', '941704620'));
  RemoveId(Result);
  CheckEquals(Doc, Result);
end;

{ TTestRemovePrimer }

procedure TTestRemovePrimer.DropCollection;
var
  Collections: TArray<String>;
  Index: Integer;
begin
  AltersCollection;

  Collections := Database.ListCollectionNames;
  TArray.Sort<String>(Collections);
  Assert.IsTrue(TArray.BinarySearch(Collections, 'restaurants', Index));

  Database.DropCollection('restaurants');

  Collections := Database.ListCollectionNames;
  TArray.Sort<String>(Collections);
  Assert.IsFalse(TArray.BinarySearch(Collections, 'restaurants', Index));
end;

procedure TTestRemovePrimer.RemoveAllDocuments;
var
  Collection: IgoMongoCollection;
  Count: Integer;
begin
  AltersCollection;

  Collection := Database.GetCollection('restaurants');
  Count := Collection.DeleteMany(TgoMongoFilter.Empty);

  Assert.AreEqual(25359, Count);
end;

procedure TTestRemovePrimer.RemoveMatchingDocuments;
var
  Collection: IgoMongoCollection;
  Count: Integer;
begin
  AltersCollection;

  Collection := Database.GetCollection('restaurants');
  Count := Collection.DeleteMany(TgoMongoFilter.Eq('borough', 'Manhattan'));

  Assert.AreEqual(10259, Count);
end;

{ TTestUpdatePrimer }

procedure TTestUpdatePrimer.UpdateEmbeddedField;
var
  Collection: IgoMongoCollection;
  Filter: TgoMongoFilter;
  Doc, Address: TgoBsonDocument;
begin
  AltersCollection;

  Collection := Database.GetCollection('restaurants');
  Filter := TgoMongoFilter.Eq('restaurant_id', '41156888');

  Doc := Collection.FindOne(Filter);
  Address := Doc['address'].AsBsonDocument;
  Assert.AreEqual(Address['street'].AsString, 'East 31 Street');

  Assert.IsTrue(Collection.UpdateOne(Filter,
    TgoMongoUpdate.Init.&Set('address.street', 'East 31st Street')));

  Doc := Collection.FindOne(Filter);
  Address := Doc['address'].AsBsonDocument;
  Assert.AreEqual(Address['street'].AsString, 'East 31st Street');
end;

procedure TTestUpdatePrimer.UpdateMultipleDocuments;
var
  Collection: IgoMongoCollection;
  FilterOld, FilterNew: TgoMongoFilter;
begin
  AltersCollection;

  Collection := Database.GetCollection('restaurants');

  FilterOld := TgoMongoFilter.Eq('address.zipcode', '10016')
           and TgoMongoFilter.Eq('cuisine', 'Other');

  FilterNew := TgoMongoFilter.Eq('address.zipcode', '10016')
           and TgoMongoFilter.Eq('cuisine', 'Category To Be Determined');

  Assert.AreEqual(20, Collection.Count(FilterOld));
  Assert.AreEqual(0, Collection.Count(FilterNew));

  Assert.AreEqual(20, Collection.UpdateMany(FilterOld,
    TgoMongoUpdate.Init
      .&Set('cuisine', 'Category To Be Determined')
      .CurrentDate('lastModified')));

  Assert.AreEqual(0, Collection.Count(FilterOld));
  Assert.AreEqual(20, Collection.Count(FilterNew));
end;

procedure TTestUpdatePrimer.UpdateTopLevelFields;
var
  Collection: IgoMongoCollection;
  Doc: TgoBsonDocument;
begin
  AltersCollection;

  Collection := Database.GetCollection('restaurants');

  Doc := Collection.FindOne(TgoMongoFilter.Eq('name', 'Juni'));
  Assert.AreEqual(Doc['cuisine'].AsString, 'American ');

  Assert.IsTrue(Collection.UpdateOne(
    TgoMongoFilter.Eq('name', 'Juni'),
    TgoMongoUpdate.Init
      .&Set('cuisine', 'American (New)')
      .CurrentDate('lastModified')));

  Doc := Collection.FindOne(TgoMongoFilter.Eq('name', 'Juni'));
  Assert.AreEqual(Doc['cuisine'].AsString, 'American (New)');
end;

initialization
  TDUnitX.RegisterTestFixture(TTestDocumentationSamples);
  TDUnitX.RegisterTestFixture(TTestQueryPrimer);
  TDUnitX.RegisterTestFixture(TTestInsertPrimer);
  TDUnitX.RegisterTestFixture(TTestRemovePrimer);
  TDUnitX.RegisterTestFixture(TTestUpdatePrimer);

end.
