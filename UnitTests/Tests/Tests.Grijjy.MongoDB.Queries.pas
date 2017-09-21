unit Tests.Grijjy.MongoDB.Queries;

interface

uses
  DUnitX.TestFramework,
  Grijjy.Bson,
  Grijjy.MongoDB.Queries;

type
  TTestMongoFilter = class
  private
    procedure CheckEquals(const AExpected: String;
      const AFilter: TgoMongoFilter); overload;
    procedure CheckEquals(const AExpected: TgoBsonDocument;
      const AFilter: TgoMongoFilter); overload;
  public
    [Test] procedure All;
    [Test] procedure &And;
    [Test] procedure AndWithClashingKeysShouldGetPromotedToDollarForm;
    [Test] procedure AndWithClashingKeysButDifferentOperatorsShouldGetMerged;
    [Test] procedure AndWithAnEmptyFilter;
    [Test] procedure AndWithNestedAndShouldGetFlattened;
    [Test] procedure AndWithNestedAndAndClashingKeys;
    [Test] procedure AndWithNestedAndAndClashingOperatorsOnTheSameKeys;
    [Test] procedure AndWithNestedAndAndClashingKeysUsingOperator;
    [Test] procedure BitsAllClear;
    [Test] procedure BitsAllSet;
    [Test] procedure BitsAnyClear;
    [Test] procedure BitsAnySet;
    [Test] procedure ElemMatch;
    [Test] procedure ElemMatchOverDictionaryRepresentedAsArrayOfDocuments;
    [Test] procedure TestEmpty;
    [Test] procedure TestExists;
    [Test] procedure Eq;
    [Test] procedure Gt;
    [Test] procedure Gte;
    [Test] procedure &In;
    [Test] procedure Lt;
    [Test] procedure Lte;
    [Test] procedure &Mod;
    [Test] procedure Ne;
    [Test] procedure Nin;
    [Test] procedure NotWithAnd;
    [Test] procedure NotWithEqual;
    [Test] procedure NotWithExists;
    [Test] procedure NotWithIn;
    [Test] procedure NotWithNot;
    [Test] procedure NotWithNotEqual;
    [Test] procedure NotWithNotIn;
    [Test] procedure NotWithNotOr;
    [Test] procedure NotWithOr;
    [Test] procedure &Or;
    [Test] procedure OrShouldFlattenNestedOrs;
    [Test] procedure Regex;
    [Test] procedure Size;
    [Test] procedure SizeGt;
    [Test] procedure SizeGte;
    [Test] procedure SizeLt;
    [Test] procedure SizeLte;
    [Test] procedure Text;
    [Test] procedure &Type;
    [Test] procedure TypeString;
  end;

type
  TTestMongoProjection = class
  private
    procedure CheckEquals(const AExpected: String;
      const AProjection: TgoMongoProjection); overload;
    procedure CheckEquals(const AExpected: TgoBsonDocument;
      const AProjection: TgoMongoProjection); overload;
  public
    [Test] procedure Combine;
    [Test] procedure CombineOperator;
    [Test] procedure CombineWithRedundantFields;
    [Test] procedure CombineWithArray;
    [Test] procedure ElemMatch;
    [Test] procedure Exclude;
    [Test] procedure ExcludeArray;
    [Test] procedure Include;
    [Test] procedure IncludeArray;
    [Test] procedure MetaTextScore;
    [Test] procedure Slice;
    [Test] procedure SliceWithSkip;
  end;

type
  TTestMongoSort = class
  private
    procedure CheckEquals(const AExpected: String;
      const ASort: TgoMongoSort); overload;
    procedure CheckEquals(const AExpected: TgoBsonDocument;
      const ASort: TgoMongoSort); overload;
  public
    [Test] procedure Ascending;
    [Test] procedure Combine;
    [Test] procedure CombineOperator;
    [Test] procedure CombineWithRepeatedFields;
    [Test] procedure Descending;
    [Test] procedure MetaTextScore;
  end;

type
  TTestMongoUpdate = class
  private
    procedure CheckEquals(const AExpected: String;
      const AUpdate: TgoMongoUpdate); overload;
    procedure CheckEquals(const AExpected: TgoBsonDocument;
      const AUpdate: TgoMongoUpdate); overload;
  public
    [Test] procedure AddToSet;
    [Test] procedure AddToSetEach;
    [Test] procedure BitwiseAnd;
    [Test] procedure BitwiseOr;
    [Test] procedure BitwiseXor;
    [Test] procedure Combine;
    [Test] procedure CurrentDate;
    [Test] procedure CurrentDateWithDateType;
    [Test] procedure CurrentDateWithTimestampType;
    [Test] procedure Inc;
    [Test] procedure IncByOne;
    [Test] procedure Max;
    [Test] procedure Min;
    [Test] procedure Mul;
    [Test] procedure PopFirst;
    [Test] procedure PopLast;
    [Test] procedure Pull;
    [Test] procedure PullAll;
    [Test] procedure PullFilter;
    [Test] procedure Push;
    [Test] procedure PushEach;
    [Test] procedure Rename;
    [Test] procedure &Set;
    [Test] procedure SetOnInsert;
    [Test] procedure Unset;
  end;

implementation

{ TTestMongoFilter }

procedure TTestMongoFilter.&And;
begin
  CheckEquals('{a: 1, b: 2}',
    TgoMongoFilter.&And(TgoMongoFilter.Eq('a', 1), TgoMongoFilter.Eq('b', 2)));
end;

procedure TTestMongoFilter.AndWithAnEmptyFilter;
begin
  CheckEquals('{a: 10}',
    TgoMongoFilter.&And('{}', TgoMongoFilter.Eq('a', 10)));
end;

procedure TTestMongoFilter.AndWithClashingKeysButDifferentOperatorsShouldGetMerged;
begin
  CheckEquals('{a: {$gt: 1, $lt: 10}}',
    TgoMongoFilter.&And(TgoMongoFilter.Gt('a', 1), TgoMongoFilter.Lt('a', 10)));
end;

procedure TTestMongoFilter.AndWithClashingKeysShouldGetPromotedToDollarForm;
begin
  CheckEquals('{$and: [{a: 1}, {a: 2}]}',
    TgoMongoFilter.&And(TgoMongoFilter.Eq('a', 1), TgoMongoFilter.Eq('a', 2)));
end;

procedure TTestMongoFilter.AndWithNestedAndAndClashingKeys;
begin
  CheckEquals('{$and: [{a: 1}, {a: 2}, {c: 3}]}',
    TgoMongoFilter.And(
      TgoMongoFilter.And(TgoMongoFilter.Eq('a', 1), TgoMongoFilter.Eq('a', 2)),
      TgoMongoFilter.Eq('c', 3)));
end;

procedure TTestMongoFilter.AndWithNestedAndAndClashingKeysUsingOperator;
begin
  CheckEquals('{$and: [{a: 1}, {a: 2}, {c: 3}]}',
    TgoMongoFilter.Eq('a', 1) and '{a: 2}' and TgoBsonDocument.Create('c', 3));
end;

procedure TTestMongoFilter.AndWithNestedAndAndClashingOperatorsOnTheSameKeys;
begin
  CheckEquals('{$and: [{a: {$lt: 1}}, {a: {$lt: 2}}]}',
    TgoMongoFilter.Lt('a', 1) and TgoMongoFilter.Lt('a', 2));
end;

procedure TTestMongoFilter.AndWithNestedAndShouldGetFlattened;
begin
  CheckEquals('{a: 1, b: 2, c: 3}',
    TgoMongoFilter.&And(
      TgoMongoFilter.&And('{a: 1}', TgoBsonDocument.Create('b', 2)),
      TgoMongoFilter.Eq('c', 3)));
end;

procedure TTestMongoFilter.BitsAllClear;
begin
  CheckEquals('{a: {$bitsAllClear: 43}}', TgoMongoFilter.BitsAllClear('a', 43));
end;

procedure TTestMongoFilter.BitsAllSet;
begin
  CheckEquals('{a: {$bitsAllSet: 43}}', TgoMongoFilter.BitsAllSet('a', 43));
end;

procedure TTestMongoFilter.BitsAnyClear;
begin
  CheckEquals('{a: {$bitsAnyClear: 43}}', TgoMongoFilter.BitsAnyClear('a', 43));
end;

procedure TTestMongoFilter.BitsAnySet;
begin
  CheckEquals('{a: {$bitsAnySet: 43}}', TgoMongoFilter.BitsAnySet('a', 43));
end;

procedure TTestMongoFilter.&In;
begin
  CheckEquals('{x: {$in: [10,20]}}', TgoMongoFilter.In('x', [10, 20]));
end;

procedure TTestMongoFilter.&Mod;
begin
  CheckEquals('{x: {$mod: [NumberLong(10), NumberLong(4)]}}', TgoMongoFilter.&Mod('x', 10, 4));
end;

procedure TTestMongoFilter.Ne;
begin
  CheckEquals('{x: {$ne: 10}}', TgoMongoFilter.Ne('x', 10));
  CheckEquals('{x: {$ne: 10}}', TgoMongoFilter.AnyNe('x', 10));
end;

procedure TTestMongoFilter.Nin;
begin
  CheckEquals('{x: {$nin: [10,20]}}', TgoMongoFilter.Nin('x', [10, 20]));
end;

procedure TTestMongoFilter.&Or;
begin
  CheckEquals('{$or: [{a: 1}, {b: 2}]}',
    TgoMongoFilter.Eq('a', 1) or TgoMongoFilter.Eq('b', 2));
end;

procedure TTestMongoFilter.OrShouldFlattenNestedOrs;
begin
  CheckEquals('{$or: [{a: 1}, {b: 2}, {c: 3}]}',
    TgoMongoFilter.Eq('a', 1) or TgoMongoFilter.Eq('b', 2) or TgoMongoFilter.Eq('c', 3));

  CheckEquals('{$or: [{a: 1}, {b: 2}, {c: 3}]}',
    TgoMongoFilter.Or([TgoMongoFilter.Eq('a', 1), TgoMongoFilter.Eq('b', 2), TgoMongoFilter.Eq('c', 3)]));
end;

procedure TTestMongoFilter.Regex;
begin
  CheckEquals('{x: /abc/}', TgoMongoFilter.Regex('x', '/abc/'));
end;

procedure TTestMongoFilter.Size;
begin
  CheckEquals('{x: {$size: 10}}', TgoMongoFilter.Size('x', 10));
end;

procedure TTestMongoFilter.SizeGt;
begin
  CheckEquals('{"x.10": {$exists: true}}', TgoMongoFilter.SizeGt('x', 10));
end;

procedure TTestMongoFilter.SizeGte;
begin
  CheckEquals('{"x.9": {$exists: true}}', TgoMongoFilter.SizeGte('x', 10));
end;

procedure TTestMongoFilter.SizeLt;
begin
  CheckEquals('{"x.9": {$exists: false}}', TgoMongoFilter.SizeLt('x', 10));
end;

procedure TTestMongoFilter.SizeLte;
begin
  CheckEquals('{"x.10": {$exists: false}}', TgoMongoFilter.SizeLte('x', 10));
end;

procedure TTestMongoFilter.&Type;
begin
  CheckEquals('{x: {$type: 2}}', TgoMongoFilter.&Type('x', TgoBsonType.String));
end;

procedure TTestMongoFilter.TypeString;
begin
  CheckEquals('{x: {$type: "string"}}', TgoMongoFilter.&Type('x', 'string'));
end;

procedure TTestMongoFilter.All;
begin
  CheckEquals('{x: {$all: [10,20]}}', TgoMongoFilter.All('x', [10, 20]));
end;

procedure TTestMongoFilter.CheckEquals(const AExpected: String;
  const AFilter: TgoMongoFilter);
begin
  CheckEquals(TgoBsonDocument.Parse(AExpected), AFilter);
end;

procedure TTestMongoFilter.CheckEquals(const AExpected: TgoBsonDocument;
  const AFilter: TgoMongoFilter);
var
  RenderedFilter: TgoBsonDocument;
begin
  RenderedFilter := AFilter.Render;
  Assert.IsTrue(RenderedFilter = AExpected);
  Assert.AreEqual(RenderedFilter.ToJson, AExpected.ToJson);

  Assert.AreEqual(AExpected.ToJson, AFilter.ToJson);
end;

procedure TTestMongoFilter.ElemMatch;
begin
  CheckEquals('{a: {$elemMatch: {b: 1}}}',
    TgoMongoFilter.ElemMatch('a', TgoMongoFilter.Eq('b', 1)));
end;

procedure TTestMongoFilter.ElemMatchOverDictionaryRepresentedAsArrayOfDocuments;
begin
  CheckEquals('{Enabled: {$elemMatch: { k: 0, v: true}}}',
    TgoMongoFilter.ElemMatch('Enabled',
      TgoMongoFilter.Eq('k', 0) and TgoMongoFilter.Eq('v', True)));
end;

procedure TTestMongoFilter.Eq;
begin
  CheckEquals('{x: 10}', TgoMongoFilter.Eq('x', 10));
  CheckEquals('{x: 10}', TgoMongoFilter.AnyEq('x', 10));
end;

procedure TTestMongoFilter.Gt;
begin
  CheckEquals('{x: {$gt: 10}}', TgoMongoFilter.Gt('x', 10));
  CheckEquals('{x: {$gt: 10}}', TgoMongoFilter.AnyGt('x', 10));
end;

procedure TTestMongoFilter.Gte;
begin
  CheckEquals('{x: {$gte: 10}}', TgoMongoFilter.Gte('x', 10));
  CheckEquals('{x: {$gte: 10}}', TgoMongoFilter.AnyGte('x', 10));
end;

procedure TTestMongoFilter.Lt;
begin
  CheckEquals('{x: {$lt: 10}}', TgoMongoFilter.Lt('x', 10));
  CheckEquals('{x: {$lt: 10}}', TgoMongoFilter.AnyLt('x', 10));
end;

procedure TTestMongoFilter.Lte;
begin
  CheckEquals('{x: {$lte: 10}}', TgoMongoFilter.Lte('x', 10));
  CheckEquals('{x: {$lte: 10}}', TgoMongoFilter.AnyLte('x', 10));
end;

procedure TTestMongoFilter.NotWithAnd;
begin
  CheckEquals('{$nor: [{$and: [{a: 1}, {a: 2}]}]}',
    not (TgoMongoFilter.Eq('a', 1) and TgoMongoFilter.Eq('a', 2)));
end;

procedure TTestMongoFilter.NotWithEqual;
begin
  CheckEquals('{a: {$ne: 1}}', not TgoMongoFilter.Eq('a', 1));
end;

procedure TTestMongoFilter.NotWithExists;
begin
  CheckEquals('{a: {$exists: false}}', not TgoMongoFilter.Exists('a'));
  CheckEquals('{a: {$exists: true}}', not TgoMongoFilter.Exists('a', False));
end;

procedure TTestMongoFilter.NotWithIn;
begin
  CheckEquals('{a: {$nin: [10, 20]}}', TgoMongoFilter.&Not(
    TgoMongoFilter.&In('a', [10, 20])));
end;

procedure TTestMongoFilter.NotWithNot;
begin
  CheckEquals('{a: 1}', not not TgoMongoFilter.Eq('a', 1));
  CheckEquals('{a: 1}', not TgoMongoFilter.Not(TgoMongoFilter.Eq('a', 1)));
  CheckEquals('{a: 1}', TgoMongoFilter.Not(not TgoMongoFilter.Eq('a', 1)));
  CheckEquals('{a: 1}', TgoMongoFilter.Not(TgoMongoFilter.Not(TgoMongoFilter.Eq('a', 1))));
end;

procedure TTestMongoFilter.NotWithNotEqual;
begin
  CheckEquals('{a: 1}', not TgoMongoFilter.Ne('a', 1));
end;

procedure TTestMongoFilter.NotWithNotIn;
begin
  CheckEquals('{a: {$in: [10, 20]}}', not TgoMongoFilter.Nin('a', [10, 20]));
end;

procedure TTestMongoFilter.NotWithNotOr;
begin
  CheckEquals('{$or: [{a: 1}, {b: 2}]}', TgoMongoFilter.Not('{$nor: [{a: 1}, {b: 2}]}'));
end;

procedure TTestMongoFilter.NotWithOr;
begin
  CheckEquals('{$nor: [{a: 1}, {b: 2}]}',
    not (TgoMongoFilter.Eq('a', 1) or TgoMongoFilter.Eq('b', 2)));
end;

procedure TTestMongoFilter.TestEmpty;
begin
  CheckEquals('{}', TgoMongoFilter.Empty);
end;

procedure TTestMongoFilter.TestExists;
begin
  CheckEquals('{x: {$exists: true}}', TgoMongoFilter.Exists('x'));
  CheckEquals('{x: {$exists: false}}', TgoMongoFilter.Exists('x', False));
end;

procedure TTestMongoFilter.Text;
begin
  CheckEquals('{$text: {$search: "funny"}}', TgoMongoFilter.Text('funny'));
  CheckEquals('{$text: {$search: "funny", $language: "en"}}', TgoMongoFilter.Text('funny', [], 'en'));
  CheckEquals('{$text: {$search: "funny", $caseSensitive: true}}', TgoMongoFilter.Text('funny', [TgoMongoTextSearchOption.CaseSensitive]));
  CheckEquals('{$text: {$search: "funny", $diacriticSensitive: true}}', TgoMongoFilter.Text('funny', [TgoMongoTextSearchOption.DiacriticSensitive]));
end;

{ TTestMongoProjection }

procedure TTestMongoProjection.CheckEquals(const AExpected: String;
  const AProjection: TgoMongoProjection);
begin
  CheckEquals(TgoBsonDocument.Parse(AExpected), AProjection);
end;

procedure TTestMongoProjection.CheckEquals(const AExpected: TgoBsonDocument;
  const AProjection: TgoMongoProjection);
var
  RenderedProjection: TgoBsonDocument;
begin
  RenderedProjection := AProjection.Render;
  Assert.IsTrue(RenderedProjection = AExpected);
  Assert.AreEqual(RenderedProjection.ToJson, AExpected.ToJson);

  Assert.AreEqual(AExpected.ToJson, AProjection.ToJson);
end;

procedure TTestMongoProjection.Combine;
begin
  CheckEquals('{fn: 1, LastName: 0}', TgoMongoProjection.Combine(
    TgoMongoProjection.Include('fn'), TgoMongoProjection.Exclude('LastName')));
end;

procedure TTestMongoProjection.CombineOperator;
begin
  CheckEquals('{fn: 1, LastName: 0}',
    TgoMongoProjection.Include('fn') + TgoMongoProjection.Exclude('LastName'));
end;

procedure TTestMongoProjection.CombineWithArray;
begin
  CheckEquals('{a: 1, b: 1, c: 0, d: 0}',
    TgoMongoProjection.Include(['a', 'b']) +
    TgoMongoProjection.Exclude(['c', 'd']));
end;

procedure TTestMongoProjection.CombineWithRedundantFields;
begin
  CheckEquals('{LastName: 0, fn: 1}', TgoMongoProjection.Combine([
    TgoMongoProjection.Include('fn'),
    TgoMongoProjection.Exclude('LastName'),
    TgoMongoProjection.Include('fn')]));
end;

procedure TTestMongoProjection.ElemMatch;
begin
  CheckEquals('{a: {$elemMatch: {b: 1}}}', TgoMongoProjection.ElemMatch('a',
    TgoMongoFilter.Eq('b', 1)));
end;

procedure TTestMongoProjection.Exclude;
begin
  CheckEquals('{a: 0}', TgoMongoProjection.Exclude('a'));
end;

procedure TTestMongoProjection.ExcludeArray;
begin
  CheckEquals('{a: 0, b: 0}', TgoMongoProjection.Exclude(['a', 'b']));
end;

procedure TTestMongoProjection.Include;
begin
  CheckEquals('{a: 1}', TgoMongoProjection.Include('a'));
end;

procedure TTestMongoProjection.IncludeArray;
begin
  CheckEquals('{a: 1, b: 1}', TgoMongoProjection.Include(['a', 'b']));
end;

procedure TTestMongoProjection.MetaTextScore;
begin
  CheckEquals('{a: {$meta: "textScore"}}', TgoMongoProjection.MetaTextScore('a'));
end;

procedure TTestMongoProjection.Slice;
begin
  CheckEquals('{a: {$slice: 10}}', TgoMongoProjection.Slice('a', 10));
end;

procedure TTestMongoProjection.SliceWithSkip;
begin
  CheckEquals('{a: {$slice: [10, 20]}}', TgoMongoProjection.Slice('a', 10, 20));
end;

{ TTestMongoSort }

procedure TTestMongoSort.Ascending;
begin
  CheckEquals('{a: 1}', TgoMongoSort.Ascending('a'));
end;

procedure TTestMongoSort.CheckEquals(const AExpected: String;
  const ASort: TgoMongoSort);
begin
  CheckEquals(TgoBsonDocument.Parse(AExpected), ASort);
end;

procedure TTestMongoSort.CheckEquals(const AExpected: TgoBsonDocument;
  const ASort: TgoMongoSort);
var
  RenderedSort: TgoBsonDocument;
begin
  RenderedSort := ASort.Render;
  Assert.IsTrue(RenderedSort = AExpected);
  Assert.AreEqual(RenderedSort.ToJson, AExpected.ToJson);

  Assert.AreEqual(AExpected.ToJson, ASort.ToJson);
end;

procedure TTestMongoSort.Combine;
begin
  CheckEquals('{a: 1, b: -1, c: -1}', TgoMongoSort.Combine([
    TgoMongoSort.Ascending('a'),
    TgoMongoSort.Descending('b'),
    TgoMongoSort.Descending('c')]));
end;

procedure TTestMongoSort.CombineOperator;
begin
  CheckEquals('{a: 1, b: -1}',
    TgoMongoSort.Ascending('a') + TgoMongoSort.Descending('b'));
end;

procedure TTestMongoSort.CombineWithRepeatedFields;
begin
  CheckEquals('{b: -1, a: -1}', TgoMongoSort.Combine([
    TgoMongoSort.Ascending('a'),
    TgoMongoSort.Descending('b'),
    TgoMongoSort.Descending('a')]));
end;

procedure TTestMongoSort.Descending;
begin
  CheckEquals('{a: -1}', TgoMongoSort.Descending('a'));
end;

procedure TTestMongoSort.MetaTextScore;
begin
  CheckEquals('{awesome: {$meta: "textScore"}}', TgoMongoSort.MetaTextScore('awesome'));
end;

{ TTestMongoUpdate }

procedure TTestMongoUpdate.CheckEquals(const AExpected: String;
  const AUpdate: TgoMongoUpdate);
begin
  CheckEquals(TgoBsonDocument.Parse(AExpected), AUpdate);
end;

procedure TTestMongoUpdate.AddToSet;
begin
  CheckEquals('{$addToSet: {a: 1}}',
    TgoMongoUpdate.Init.AddToSet('a', 1));

  CheckEquals('{$addToSet: {a: [1, 2]}}',
    TgoMongoUpdate.Init.AddToSet('a', TgoBsonArray.Create([1, 2])));
end;

procedure TTestMongoUpdate.AddToSetEach;
begin
  CheckEquals('{$addToSet: {a: {$each: [1, 2]}}}',
    TgoMongoUpdate.Init.AddToSetEach('a', [1, 2]));

  CheckEquals('{$addToSet: {a: {$each: [[1, 2], [3, 4]]}}}',
    TgoMongoUpdate.Init.AddToSetEach('a', [TgoBsonArray.Create([1, 2]),
    TgoBsonArray.Create([3, 4])]));
end;

procedure TTestMongoUpdate.BitwiseAnd;
begin
  CheckEquals('{$bit: {a: {and: 1}}}', TgoMongoUpdate.Init.BitwiseAnd('a', 1));
end;

procedure TTestMongoUpdate.BitwiseOr;
begin
  CheckEquals('{$bit: {a: {or: 1}}}', TgoMongoUpdate.Init.BitwiseOr('a', 1));
end;

procedure TTestMongoUpdate.BitwiseXor;
begin
  CheckEquals('{$bit: {a: {xor: 1}}}', TgoMongoUpdate.Init.BitwiseXor('a', 1));
end;

procedure TTestMongoUpdate.CheckEquals(const AExpected: TgoBsonDocument;
  const AUpdate: TgoMongoUpdate);
var
  RenderedUpdate: TgoBsonDocument;
begin
  RenderedUpdate := AUpdate.Render;
  Assert.IsTrue(RenderedUpdate = AExpected);
  Assert.AreEqual(RenderedUpdate.ToJson, AExpected.ToJson);

  Assert.AreEqual(AExpected.ToJson, AUpdate.ToJson);
end;

procedure TTestMongoUpdate.Combine;
begin
  CheckEquals('{$set: {a: 1, b: 2}}',
    TgoMongoUpdate.Init.&Set('a', 1).&Set('b', 2));

  CheckEquals('{$set: {a: 1, b: 2, c: 3}}',
    TgoMongoUpdate.Init.&Set('a', 1).&Set('b', 2).&Set('c', 3));
end;

procedure TTestMongoUpdate.CurrentDate;
begin
  CheckEquals('{$currentDate: {a: true}}', TgoMongoUpdate.Init.CurrentDate('a'));
end;

procedure TTestMongoUpdate.CurrentDateWithDateType;
begin
  CheckEquals('{$currentDate: {a: {$type: "date"}}}',
    TgoMongoUpdate.Init.CurrentDate('a', TgoMongoCurrentDateType.Date));
end;

procedure TTestMongoUpdate.CurrentDateWithTimestampType;
begin
  CheckEquals('{$currentDate: {a: {$type: "timestamp"}}}',
    TgoMongoUpdate.Init.CurrentDate('a', TgoMongoCurrentDateType.Timestamp));
end;

procedure TTestMongoUpdate.Inc;
begin
  CheckEquals('{$inc: {a: 10}}', TgoMongoUpdate.Init.Inc('a', 10));
end;

procedure TTestMongoUpdate.IncByOne;
begin
  CheckEquals('{$inc: {a: 1}}', TgoMongoUpdate.Init.Inc('a'));
end;

procedure TTestMongoUpdate.Max;
begin
  CheckEquals('{$max: {a: 1}}', TgoMongoUpdate.Init.Max('a', 1));
end;

procedure TTestMongoUpdate.Min;
begin
  CheckEquals('{$min: {a: 1}}', TgoMongoUpdate.Init.Min('a', 1));
end;

procedure TTestMongoUpdate.Mul;
begin
  CheckEquals('{$mul: {a: 2}}', TgoMongoUpdate.Init.Mul('a', 2));
end;

procedure TTestMongoUpdate.PopFirst;
begin
  CheckEquals('{$pop: {a: -1}}', TgoMongoUpdate.Init.PopFirst('a'));
end;

procedure TTestMongoUpdate.PopLast;
begin
  CheckEquals('{$pop: {a: 1}}', TgoMongoUpdate.Init.PopLast('a'));
end;

procedure TTestMongoUpdate.Pull;
begin
  CheckEquals('{$pull: {a: 1}}', TgoMongoUpdate.Init.Pull('a', 1));
  CheckEquals('{$pull: {a: [1, 2]}}', TgoMongoUpdate.Init.Pull('a', TgoBsonArray.Create([1, 2])));
end;

procedure TTestMongoUpdate.PullAll;
begin
  CheckEquals('{$pullAll: {a: [1, 2]}}', TgoMongoUpdate.Init.PullAll('a', [1, 2]));

  CheckEquals('{$pullAll: {a: [[1, 2], [3, 4]]}}',
    TgoMongoUpdate.Init.PullAll('a', [TgoBsonArray.Create([1, 2]),
    TgoBsonArray.Create([3, 4])]));
end;

procedure TTestMongoUpdate.PullFilter;
begin
  CheckEquals('{$pull: {a: {b: {$gt: 1}}}}', TgoMongoUpdate.Init.PullFilter('a',
    TgoMongoFilter.Gt('b', 1)));
end;

procedure TTestMongoUpdate.Push;
begin
  CheckEquals('{$push: {a: 1}}', TgoMongoUpdate.Init.Push('a', 1));
  CheckEquals('{$push: {a: [1, 2]}}', TgoMongoUpdate.Init.Push('a', TgoBsonArray.Create([1, 2])));
end;

procedure TTestMongoUpdate.PushEach;
begin
  // -slice, -position, -sort
  CheckEquals('{$push: {a: {$each: [1, 2]}}}', TgoMongoUpdate.Init
    .PushEach('a', [1, 2]));

  // -slice, -position, +sort
  CheckEquals('{$push: {a: {$each: [1, 2], $sort: {b: 1}}}}', TgoMongoUpdate.Init
    .PushEach('a', [1, 2], TgoMongoSort.Ascending('b')));

  // +slice, -position, -sort
  CheckEquals('{$push: {a: {$each: [1, 2], $slice: 3}}}', TgoMongoUpdate.Init
    .PushEach('a', [1, 2], 3));

  // +slice, -position, +sort
  CheckEquals('{$push: {a: {$each: [1, 2], $slice: 3, $sort: {b: 1}}}}', TgoMongoUpdate.Init
    .PushEach('a', [1, 2], TgoMongoSort.Ascending('b'), 3));

  // -slice, +position, -sort
  CheckEquals('{$push: {a: {$each: [1, 2], $position: 4}}}', TgoMongoUpdate.Init
    .PushEach('a', [1, 2], TgoMongoUpdate.NO_SLICE, 4));

  // -slice, +position, +sort
  CheckEquals('{$push: {a: {$each: [1, 2], $position: 4, $sort: {b: 1}}}}', TgoMongoUpdate.Init
    .PushEach('a', [1, 2], TgoMongoSort.Ascending('b'), TgoMongoUpdate.NO_SLICE, 4));

  // +slice, +position, -sort
  CheckEquals('{$push: {a: {$each: [1, 2], $slice: 3, $position: 4}}}', TgoMongoUpdate.Init
    .PushEach('a', [1, 2], 3, 4));

  // +slice, +position, +sort
  CheckEquals('{$push: {a: {$each: [1, 2], $slice: 3, $position: 4, $sort: {b: 1}}}}', TgoMongoUpdate.Init
    .PushEach('a', [1, 2], TgoMongoSort.Ascending('b'), 3, 4));
end;

procedure TTestMongoUpdate.Rename;
begin
  CheckEquals('{$rename: {a: "b"}}', TgoMongoUpdate.Init.Rename('a', 'b'));
end;

procedure TTestMongoUpdate.&Set;
begin
  CheckEquals('{$set: {a: 1}}', TgoMongoUpdate.Init.&Set('a', 1));
end;

procedure TTestMongoUpdate.SetOnInsert;
begin
  CheckEquals('{$setOnInsert: {a: 1}}', TgoMongoUpdate.Init.SetOnInsert('a', 1));
end;

procedure TTestMongoUpdate.Unset;
begin
  CheckEquals('{$unset: {a: 1}}', TgoMongoUpdate.Init.Unset('a'));
end;

initialization
  TDUnitX.RegisterTestFixture(TTestMongoFilter);
  TDUnitX.RegisterTestFixture(TTestMongoProjection);
  TDUnitX.RegisterTestFixture(TTestMongoSort);
  TDUnitX.RegisterTestFixture(TTestMongoUpdate);

end.
