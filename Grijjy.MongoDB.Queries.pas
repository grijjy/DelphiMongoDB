unit Grijjy.MongoDB.Queries;
(*< Tools for building MongoDB queries.

  @bold(Quick Start)

  <source>
  var
    Filter: TgoMongoFilter;
    Projection: TgoMongoProjection;
    Sort: TgoMongoSort;
    Update: TgoMongoUpdate;
  begin
    Filter := TgoMongoFilter.Gte('age', 21) and TgoMongoFilter.Lt('age', 65');
    WriteLn(Filter.ToJson); // outputs: {age: {$gte: 21, $lt: 65}}

    Projection := TgoMongoProjection.Include('name') + TgoMongoProjection.Exclude('_id');
    WriteLn(Projection.ToJson); // outputs: {name: 1, _id: 0}

    Sort := TgoMongoSort.Decending('age') + TgoMongoSort.Ascending('posts');
    WriteLn(Sort.ToJson); // outputs: {age: -1, posts: 1}

    Update := TgoMongoUpdate.Init.&Set('age', 42).&Set('name', 'Joe');
    WriteLn(Update.ToJson); // outputs {$set: {age: 42, name: "Joe"}}
  end;
  </source>

  @bold(Query Tools)

  Filters (TgoMongoFilter), projections (TgoMongoProjection), sort modifiers
  (TgrMongoSort) and update definitions (TgrMongoUpdate) can be implictly
  converted from JSON strings and documents, or they can be created using
  static helper functions. The following 3 statements all have the same result:

  <source>
  MyFilter := '{age: {$gt: 18}}';
  MyFilter := TgoBsonDocument.Create('age', TgoBsonDocument.Create('$gt', 18));
  MyFilter := TgoMongoFilter.Gt('age', 18);
  </source>

  They can be converted to BSON documents using the Render method, or to JSON
  and BSON using to ToJson and ToBson methods.

  @bold(TgoMongoFilter)

  Comparision:
  * TgoMongoFilter.Eq
  * TgoMongoFilter.Ne
  * TgoMongoFilter.Lt
  * TgoMongoFilter.Lte
  * TgoMongoFilter.Gt
  * TgoMongoFilter.Gte

  Logical:
  * TgoMongoFilter.LogicalAnd
  * TgoMongoFilter.LogicalOr
  * TgoMongoFilter.LogicalNot
  * TgoMongoFilter.And
  * TgoMongoFilter.Or
  * TgoMongoFilter.Not

  Element:
  * TgoMongoFilter.Exists
  * TgoMongoFilter.Type

  Evaluation:
  * TgoMongoFilter.Mod
  * TgoMongoFilter.Regex
  * TgoMongoFilter.Text

  Array:
  * TgoMongoFilter.AnyEq
  * TgoMongoFilter.AnyNe
  * TgoMongoFilter.AnyLt
  * TgoMongoFilter.AnyLte
  * TgoMongoFilter.AnyGt
  * TgoMongoFilter.AnyGte
  * TgoMongoFilter.All
  * TgoMongoFilter.In
  * TgoMongoFilter.Nin
  * TgoMongoFilter.ElemMatch
  * TgoMongoFilter.Size
  * TgoMongoFilter.SizeGt
  * TgoMongoFilter.SizeGte
  * TgoMongoFilter.SizeLt
  * TgoMongoFilter.SizeLte

  Bitwise:
  * TgoMongoFilter.BitsAllClear
  * TgoMongoFilter.BitsAllSet
  * TgoMongoFilter.BitsAnyClear
  * TgoMongoFilter.BitsAnySet

  @bold(TgoMongoProjection)

  * TgoMongoProjection.Include
  * TgoMongoProjection.Exclude
  * TgoMongoProjection.ElemMatch
  * TgoMongoProjection.MetaTextScore
  * TgoMongoProjection.Slice
  * TgoMongoProjection.Add
  * TgoMongoProjection.Combine

  @bold(TgrMongoSort)

  * TgoMongoSort.Ascending
  * TgoMongoSort.Descending
  * TgoMongoSort.MetaTextScore
  * TgoMongoSort.Add
  * TgoMongoSort.Combine

  @bold(TgrMongoUpdate)

  Fields:
  * TgoMongoUpdate.Set
  * TgoMongoUpdate.SetOnInsert
  * TgoMongoUpdate.Unset
  * TgoMongoUpdate.Inc
  * TgoMongoUpdate.Mul
  * TgoMongoUpdate.Max
  * TgoMongoUpdate.Min
  * TgoMongoUpdate.CurrentDate
  * TgoMongoUpdate.Rename

  Array:
  * TgoMongoUpdate.AddToSet
  * TgoMongoUpdate.AddToSetEach
  * TgoMongoUpdate.PopFirst
  * TgoMongoUpdate.PopLast
  * TgoMongoUpdate.Pull
  * TgoMongoUpdate.PullFilter
  * TgoMongoUpdate.PullAll
  * TgoMongoUpdate.Push
  * TgoMongoUpdate.PushEach

  Bitwise:
  * TgoMongoUpdate.BitwiseAnd
  * TgoMongoUpdate.BitwiseOr
  * TgoMongoUpdate.BitwiseXor *)

{$INCLUDE 'Grijjy.inc'}

interface

uses
  System.SysUtils,
  Grijjy.Bson;

type
  { Text searching options for TgoMongoFilter.Text }
  TgoMongoTextSearchOption = (
    { The enable case sensitive search }
    CaseSensitive,

    { The enable diacritic sensitive search }
    DiacriticSensitive);

type
  { Set of text search options }
  TgoMongoTextSearchOptions = set of TgoMongoTextSearchOption;

type
  (* A MongoDB filter. A filter defines a query criterion. For example, the
     filter <tt>{ age : { $gt: 18 } }</tt> can be used to query persons more
     than 18 years old.

     Filters can be implictly converted from JSON strings and documents, or they
     can be created using static helper functions. The following 3 statements
     all have the same result:

     <source>
     MyFilter := '{age: {$gt: 18}}';
     MyFilter := TgoBsonDocument.Create('age', TgoBsonDocument.Create('$gt', 18));
     MyFilter := TgoMongoFilter.Gt('age', 18);
     </source>

     You can use the logical "and", "or" and "not" operators to combine multiple
     filters into one:

     <source>
     MyFilter := TgoMongoFilter.Gt('a', 1) and TgoMongoFilter.Lt('a', 10);
     </source>

     This results in: <tt>{a: {$gt: 1, $lt: 10}}</tt> *)
  TgoMongoFilter = record
  {$REGION 'Internal Declarations'}
  private type
    IFilter = interface
    ['{BAE9502F-7FC3-4AB4-B35F-AEA09F8BC0DB}']
      function Render: TgoBsonDocument;
      function ToBson: TBytes;
      function ToJson(const ASettings: TgoJsonWriterSettings): String;
    end;
  private class var
    FEmpty: TgoMongoFilter;
  private
    FImpl: IFilter;
  public
    class constructor Create;
  {$ENDREGION 'Internal Declarations'}
  public
    (* Implicitly converts a Json string to a filter. Use this converter if you
       have manually created a query criterion in Json (such
       as <tt>{ age : { $gt: 18 } }</tt>) and want to use it as a filter.
       The Json string must be parseable to a BSON document.

       Raises:
         EgrJsonParserError or EInvalidOperation on parse errors *)
    class operator Implicit(const AJson: String): TgoMongoFilter; static;

    (* Implicitly converts a BSON Document to a filter. Use this converter if you
       have manually created a BSON document for a query criterion, and want to
       use it as a filter. *)
    class operator Implicit(const ADocument: TgoBsonDocument): TgoMongoFilter; static;

    (* Checks if the filter has been assigned.

       Returns:
         True if the filter hasn't been assigned yet. *)
    function IsNil: Boolean; inline;

    { Unassigns the filter (like setting an object to nil).
      IsNil will return True afterwards. }
    procedure SetNil; inline;

    (* Renders the filter to a BSON Document.

       Returns:
         The BSON Document that represents this filter. *)
    function Render: TgoBsonDocument; inline;

    (* Converts the filter to binary BSON.

       Returns:
         The BSON byte stream that represents this filter. *)
    function ToBson: TBytes; inline;

    (* Converts the filter to a JSON string.

       Returns:
         This filter in JSON format.

       @bold(Note): the filter is converted using the default writer settings.
       That is, without any pretty printing, and in Strict mode. Use the other
       overload of this function to specify output settings. *)
    function ToJson: String; overload; inline;

    (* Converts the filter to a JSON string, using specified settings.

       Parameters:
         ASettings: the output settings to use, such as pretty-printing and
           Strict vs Shell mode.

       Returns:
         This filter in JSON format. *)
    function ToJson(const ASettings: TgoJsonWriterSettings): String; overload; inline;

    { Empty filter. An empty filter matches everything. }
    class property Empty: TgoMongoFilter read FEmpty;
  public
    {===================================
      Comparison
     ===================================}

    (* Matches values that are equal to a specified value.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValue: the value (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Eq('x', 1)</code>
       results in <tt>{x: 1}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/eq/#op._S_eq *)
    class function Eq(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoFilter; static;

    (* Matches values that are @bold(not) equal to a specified value.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValue: the value (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Ne('x', 1)</code>
       results in <tt>{x: {$ne: 1}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/ne/#op._S_ne *)
    class function Ne(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoFilter; static;

    (* Matches values that are less than a specified value.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValue: the value (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Lt('x', 1)</code>
       results in <tt>{x: {$lt: 1}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/lt/#op._S_lt *)
    class function Lt(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoFilter; static;

    (* Matches values that are less than or equal to a specified value.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValue: the value (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Lte('x', 1)</code>
       results in <tt>{x: {$lte: 1}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/lte/#op._S_lte *)
    class function Lte(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoFilter; static;

    (* Matches values that are greater than a specified value.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValue: the value (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Gt('x', 1)</code>
       results in <tt>{x: {$gt: 1}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/gt/#op._S_gt *)
    class function Gt(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoFilter; static;

    (* Matches values that are greater than or equal to a specified value.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValue: the value (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Gte('x', 1)</code>
       results in <tt>{x: {$gte: 1}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/gte/#op._S_gte *)
    class function Gte(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoFilter; static;
  public
    {===================================
      Logical
     ===================================}

    (* Joins query clauses with a logical AND returns all documents that match
       the conditions of both clauses. For example:

       <source>
       Filter := TgoMongoFilter.Eq('x', 1) and TgoMongoFilter.Eq('y', 2);
       </source>

       will result in <tt>{x: 1, y: 2}</tt>.

       Depending on the source filters, the output may be different.
       For example:

       <source>
       Filter := TgoMongoFilter.Gt('x', 1) and TgoMongoFilter.Lt('x', 10);
       </source>

       will result in <tt>{x: {$gt: 1, $lt: 10}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/and/#op._S_and *)
    class operator LogicalAnd(const ALeft, ARight: TgoMongoFilter): TgoMongoFilter; static;

    (* Joins query clauses with a logical OR returns all documents that match
       the conditions of either clause. For example:

       <source>
       Filter := TgoMongoFilter.Eq('x', 1) or TgoMongoFilter.Eq('y', 2);
       </source>

       will result in <tt>{$or: [{x: 1}, {y: 2}]}</tt>.

       See https://docs.mongodb.org/manual/reference/operator/query/or/#op._S_or *)
    class operator LogicalOr(const ALeft, ARight: TgoMongoFilter): TgoMongoFilter; static;

    (* Inverts the effect of a query expression and returns documents that do
       not match the query expression. For example:

       <source>
       Filter := not TgoMongoFilter.Eq('x', 1);
       </source>

       will result in <tt>{x: {$ne: 1}}</tt>.

       See https://docs.mongodb.org/manual/reference/operator/query/not/#op._S_not *)
    class operator LogicalNot(const AOperand: TgoMongoFilter): TgoMongoFilter; static;

    (* Joins query clauses with a logical AND returns all documents that match
       the conditions of both clauses. For example:

       Parameters:
         AFilter1: the first filter.
         AFilter2: the second filter.

       Returns:
         The AND filter.

       <source>
       Filter := TgoMongoFilter.&And(
         TgoMongoFilter.Eq('x', 1),
         TgoMongoFilter.Eq('y', 2));
       </source>

       will result in <tt>{x: 1, y: 2}</tt>.

       Depending on the source filters, the output may be different.
       For example:

       <source>
       Filter := TgoMongoFilter.&And(
         TgoMongoFilter.Gt('x', 1),
         TgoMongoFilter.Lt('x', 10));
       </source>

       will result in <tt>{x: {$gt: 1, $lt: 10}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/and/#op._S_and *)
    class function &And(const AFilter1,
      AFilter2: TgoMongoFilter): TgoMongoFilter; overload; static;

    (* Joins query clauses with a logical AND returns all documents that match
       the conditions of both clauses. For example:

       Parameters:
         AFilters: the filters to combine.

       Returns:
         The AND filter.

       <source>
       Filter := TgoMongoFilter.&And([
         TgoMongoFilter.Eq('x', 1),
         TgoMongoFilter.Eq('y', 2),
         TgoMongoFilter.Eq('z', 3)]);
       </source>

       will result in <tt>{x: 1, y: 2, z: 3}</tt>.

       Depending on the source filter, the output may be different. For example:

       <source>
       Filter := TgoMongoFilter.&And([
         TgoMongoFilter.Eq('x', 1),
         TgoMongoFilter.Eq('x', 2),
         TgoMongoFilter.Eq('z', 3)]);
       </source>

       will result in <tt>{$and: [{x: 1}, {x: 2}, {z: 3}]}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/and/#op._S_and *)
    class function &And(
      const AFilters: array of TgoMongoFilter): TgoMongoFilter; overload; static;

    (* Joins query clauses with a logical OR returns all documents that match
       the conditions of either clause. For example:

       Parameters:
         AFilter1: the first filter.
         AFilter2: the second filter.

       Returns:
         The OR filter.

       <source>
       Filter := TgoMongoFilter.Eq('x', 1) or TgoMongoFilter.Eq('y', 2);
       </source>

       will result in <tt>{$or: [{x: 1}, {y: 2}]}</tt>.

       See https://docs.mongodb.org/manual/reference/operator/query/or/#op._S_or *)
    class function &Or(const AFilter1,
      AFilter2: TgoMongoFilter): TgoMongoFilter; overload; static;

    (* Joins query clauses with a logical OR returns all documents that match
       the conditions of either clause. For example:

       Parameters:
         AFilters: the filters to combine.

       Returns:
         The OR filter.

       <source>
       Filter := TgoMongoFilter.Eq('x', 1) or TgoMongoFilter.Eq('y', 2);
       </source>

       will result in <tt>{$or: [{x: 1}, {y: 2}]}</tt>.

       See https://docs.mongodb.org/manual/reference/operator/query/or/#op._S_or *)
    class function &Or(
      const AFilters: array of TgoMongoFilter): TgoMongoFilter; overload; static;

    (* Inverts the effect of a query expression and returns documents that do
       not match the query expression. For example:

       Parameters:
         AOperand: the filter to negate.

       Returns:
         The NOT filter.

       <source>
       Filter := TgoMongoFilter.&Not(TgoMongoFilter.Eq('x', 1));
       </source>

       will result in <tt>{x: {$ne: 1}}</tt>.

       See https://docs.mongodb.org/manual/reference/operator/query/not/#op._S_not *)
    class function &Not(const AOperand: TgoMongoFilter): TgoMongoFilter; static;
  public
    {===================================
      Element
     ===================================}

    (* Matches documents that have (or have not) the specified field.

       Parameters:
         AFieldName: the name of the field (left operand).
         AExists: (optional) whether the field must exist or not (right
           operand). Defaults to True.

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Exists('x')</code>
       results in <tt>{x: {$exists: true}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/exists/#op._S_exists *)
    class function Exists(const AFieldName: String;
      const AExists: Boolean = True): TgoMongoFilter; static;

    (* Selects documents if a field is of the specified type.

       Parameters:
         AFieldName: the name of the field (left operand).
         AType: the BSON type to match (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.&Type('x', TgtBsonType.&String)</code>
       results in <tt>{x: {$type: 2}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/type/#op._S_type *)
    class function &Type(const AFieldName: String;
      const AType: TgoBsonType): TgoMongoFilter; overload; static;

    (* Selects documents if a field is of the specified type.

       Parameters:
         AFieldName: the name of the field (left operand).
         AType: the string alias for the BSON type to match (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.&Type('x', 'string')</code>
       results in <tt>{x: {$type: "string"}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/type/#op._S_type *)
    class function &Type(const AFieldName, AType: String): TgoMongoFilter; overload; static;
  public
    {===================================
      Evaluation
     ===================================}

    (* Select documents where the value of a field divided by a divisor has the
       specified remainder (i.e. perform a modulo operation to select documents).

       Parameters:
         AFieldName: the name of the field (left operand).
         ADivisor: the divisor.
         ARemainder: the remainder.

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.&Mod('x', 10, 4)</code>
       results in <tt>{x: {$mod: [NumberLong(10), NumberLong(4)]}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/mod/#op._S_mod *)
    class function &Mod(const AFieldName: String;
      const ADivisor, ARemainder: Int64): TgoMongoFilter; static;

    (* Selects documents where values match a specified regular expression.

       Parameters:
         AFieldName: the name of the field (left operand).
         ARegex: the regular expression.

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Regex('x', '/abc/')</code>
       results in <tt>{x: /abc/}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/regex/#op._S_regex *)
    class function Regex(const AFieldName: String;
      const ARegex: TgoBsonRegularExpression): TgoMongoFilter; static;

    (* Performs a text search.

       Parameters:
         AText: a string of terms that MongoDB parses and uses to query the text
           index. MongoDB performs a logical OR search of the terms unless
           specified as a phrase.
         AOptions: (optional) text search options. Defaults to none.
         ALanguage: (optional) language that determines the list of stop words
           for the search and the rules for the stemmer and tokenizer. If not
           specified, the search uses the default language of the index.

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Text('funny', [TgrMongoTextSearchOption.CaseSensitive])</code>
       results in <tt>{$text: {$search: "funny", $caseSensitive: true}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/text/#op._S_text *)
    class function Text(const AText: String;
      const AOptions: TgoMongoTextSearchOptions = [];
      const ALanguage: String = ''): TgoMongoFilter; static;
  public
    {===================================
      Array
     ===================================}

    (* Matches values where an array field contains any element with a
       specific value.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValue: the value (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.AnyEq('x', 1)</code>
       results in <tt>{x: 1}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/eq/#op._S_eq *)
    class function AnyEq(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoFilter; static;

    (* Matches values where an array field contains any element not qual to a
       specific value.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValue: the value (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.AnyNe('x', 1)</code>
       results in <tt>{x: {$ne: 1}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/ne/#op._S_ne *)
    class function AnyNe(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoFilter; static;

    (* Matches values where an array field contains any element with a value
       less than a specific value.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValue: the value (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.AnyLt('x', 1)</code>
       results in <tt>{x: {$lt: 1}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/lt/#op._S_lt *)
    class function AnyLt(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoFilter; static;

    (* Matches values where an array field contains any element with a value
       less than or equal to a specific value.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValue: the value (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.AnyLte('x', 1)</code>
       results in <tt>{x: {$lte: 1}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/lte/#op._S_lte *)
    class function AnyLte(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoFilter; static;

    (* Matches values where an array field contains any element with a value
       greater than a specific value.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValue: the value (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.AnyGt('x', 1)</code>
       results in <tt>{x: {$gt: 1}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/gt/#op._S_gt *)
    class function AnyGt(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoFilter; static;

    (* Matches values where an array field contains any element with a value
       greater than or equal to a specific value.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValue: the value (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.AnyGte('x', 1)</code>
       results in <tt>{x: {$gte: 1}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/gte/#op._S_gte *)
    class function AnyGte(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoFilter; static;

    (* Creates an All filter for an array field.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValues: the array values (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.All('x', [1, 2])</code>
       results in <tt>{x: {$all: [1, 2]}}</tt> *)
    class function All(const AFieldName: String;
      const AValues: TArray<TgoBsonValue>): TgoMongoFilter; overload; static;

    (* Matches arrays that contain all elements specified in the query.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValues: the array values (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.All('x', [1, 2])</code>
       results in <tt>{x: {$all: [1, 2]}}</tt> *)
    class function All(const AFieldName: String;
      const AValues: array of TgoBsonValue): TgoMongoFilter; overload; static;

    (* Matches arrays that contain all elements specified in the query.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValues: the BSON Array (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.All('x', TgoBsonArray.Create([1, 2]))</code>
       results in <tt>{x: {$all: [1, 2]}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/all/#op._S_all *)
    class function All(const AFieldName: String;
      const AValues: TgoBsonArray): TgoMongoFilter; overload; static;

    (* Matches any of the values specified in an array.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValues: the array values (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.In('x', [1, 2])</code>
       results in <tt>{x: {$in: [1, 2]}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/in/#op._S_in *)
    class function &In(const AFieldName: String;
      const AValues: TArray<TgoBsonValue>): TgoMongoFilter; overload; static;

    (* Matches any of the values specified in an array.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValues: the array values (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.In('x', [1, 2])</code>
       results in <tt>{x: {$in: [1, 2]}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/in/#op._S_in *)
    class function &In(const AFieldName: String;
      const AValues: array of TgoBsonValue): TgoMongoFilter; overload; static;

    (* Matches any of the values specified in an array.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValues: the BSON Array (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.In('x', TgoBsonArray.Create([1, 2]))</code>
       results in <tt>{x: {$in: [1, 2]}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/in/#op._S_in *)
    class function &In(const AFieldName: String;
      const AValues: TgoBsonArray): TgoMongoFilter; overload; static;

    (* Matches none of the values specified in an array.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValues: the array values (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Nin('x', [1, 2])</code>
       results in <tt>{x: {$nin: [1, 2]}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/nin/#op._S_nin *)
    class function Nin(const AFieldName: String;
      const AValues: TArray<TgoBsonValue>): TgoMongoFilter; overload; static;

    (* Matches none of the values specified in an array.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValues: the array values (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Nin('x', [1, 2])</code>
       results in <tt>{x: {$nin: [1, 2]}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/nin/#op._S_nin *)
    class function Nin(const AFieldName: String;
      const AValues: array of TgoBsonValue): TgoMongoFilter; overload; static;

    (* Matches none of the values specified in an array.

       Parameters:
         AFieldName: the name of the field (left operand).
         AValues: the BSON Array (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Nin('x', TgoBsonArray.Create([1, 2]))</code>
       results in <tt>{x: {$nin: [1, 2]}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/nin/#op._S_nin *)
    class function Nin(const AFieldName: String;
      const AValues: TgoBsonArray): TgoMongoFilter; overload; static;

    (* Selects documents if element in the array field matches all the specified
       conditions.

       Parameters:
         AFieldName: the name of the field (left operand).
         AFilter: the filter specifying the conditions (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.ElemMatch('Enabled', TgoMongoFilter.Eq('k', 0) and TgoMongoFilter.Eq('v', True))</code>
       results in <tt>{Enabled: {$elemMatch: { k: 0, v: true}}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/elemMatch/#op._S_elemMatch *)
    class function ElemMatch(const AFieldName: String;
      const AFilter: TgoMongoFilter): TgoMongoFilter; overload; static;

    (* Selects documents if the array field is a specified size.

       Parameters:
         AFieldName: the name of the field (left operand).
         ASize: the size (aka length or count) of the array (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.Size('x', 10)</code>
       results in <tt>{x: {$size: 10}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/size/#op._S_size *)
    class function Size(const AFieldName: String;
      const ASize: Integer): TgoMongoFilter; overload; static;

    (* Selects documents if the array field is greater than a specified size.

       Parameters:
         AFieldName: the name of the field (left operand).
         ASize: the size (aka length or count) of the array (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.SizeGt('x', 10)</code>
       results in <tt>{"x.10": {$exists: true}}</tt> *)
    class function SizeGt(const AFieldName: String;
      const ASize: Integer): TgoMongoFilter; overload; static;

    (* Selects documents if the array field is greater than or equal to a
       specified size.

       Parameters:
         AFieldName: the name of the field (left operand).
         ASize: the size (aka length or count) of the array (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.SizeGt('x', 10)</code>
       results in <tt>{"x.9": {$exists: true}}</tt> *)
    class function SizeGte(const AFieldName: String;
      const ASize: Integer): TgoMongoFilter; overload; static;

    (* Selects documents if the array field is less than a specified size.

       Parameters:
         AFieldName: the name of the field (left operand).
         ASize: the size (aka length or count) of the array (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.SizeLt('x', 10)</code>
       results in <tt>{"x.9": {$exists: false}}</tt> *)
    class function SizeLt(const AFieldName: String;
      const ASize: Integer): TgoMongoFilter; overload; static;

    (* Selects documents if the array field is less than or equal to a
       specified size.

       Parameters:
         AFieldName: the name of the field (left operand).
         ASize: the size (aka length or count) of the array (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.SizeLte('x', 10)</code>
       results in <tt>{"x.10": {$exists: false}}</tt> *)
    class function SizeLte(const AFieldName: String;
      const ASize: Integer): TgoMongoFilter; overload; static;
  public
    {===================================
      Bitwise
     ===================================}

    (* Matches numeric or binary values in which a set of bit positions
       @bold(all) have a value of 0.

       Parameters:
         AFieldName: the name of the field (left operand).
         ABitMask: the set of bit positions (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.BitsAllClear('x', 43)</code>
       results in <tt>{x: {$bitsAllClear: 43}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/bitsAllClear/#op._S_bitsAllClear *)
    class function BitsAllClear(const AFieldName: String;
      const ABitMask: UInt64): TgoMongoFilter; static;

    (* Matches numeric or binary values in which a set of bit positions
       @bold(all) have a value of 1.

       Parameters:
         AFieldName: the name of the field (left operand).
         ABitMask: the set of bit positions (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.BitsAllSet('x', 43)</code>
       results in <tt>{x: {$bitsAllSet: 43}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/bitsAllSet/#op._S_bitsAllSet *)
    class function BitsAllSet(const AFieldName: String;
      const ABitMask: UInt64): TgoMongoFilter; static;

    (* Matches numeric or binary values in which @bold(any) bit from a set of
       bit positions has a value of 0.

       Parameters:
         AFieldName: the name of the field (left operand).
         ABitMask: the set of bit positions (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.BitsAnyClear('x', 43)</code>
       results in <tt>{x: {$bitsAnyClear: 43}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/bitsAnyClear/#op._S_bitsAnyClear *)
    class function BitsAnyClear(const AFieldName: String;
      const ABitMask: UInt64): TgoMongoFilter; static;

    (* Matches numeric or binary values in which @bold(any) bit from a set of
       bit positions has a value of 1.

       Parameters:
         AFieldName: the name of the field (left operand).
         ABitMask: the set of bit positions (right operand).

       Returns:
         The filter.

       Example: <code>TgoMongoFilter.BitsAnyClear('x', 43)</code>
       results in <tt>{x: {$bitsAnyClear: 43}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/query/bitsAnySet/#op._S_bitsAnySet *)
    class function BitsAnySet(const AFieldName: String;
      const ABitMask: UInt64): TgoMongoFilter; static;
  end;

type
  (* A MongoDB projection. A projection limits the amount of data that MongoDB
     sends to application. For example, the projection <tt>{ _id: 0 }</tt>
     excludes the _id field from result sets.

     Projections can be implictly converted from JSON strings and documents, or
     they can be created using static helper functions. The following 3
     statements all have the same result:

     <source>
     MyProjection := '{_id: 0}';
     MyProjection := TgoBsonDocument.Create('_id', 0);
     MyProjection := TgoMongoProjection.Exclude('_id');
     </source>

     You can use the add (+) operator to combine multiple projections into one:

     <source>
     MyProjection := TgoMongoProjection.Include('name') + TgoMongoProjection.Exclude('_id');
     </source>

     This results in: <tt>{name: 1, _id: 0}</tt> *)
  TgoMongoProjection = record
  {$REGION 'Internal Declarations'}
  private type
    IProjection = interface
    ['{060E413F-6B4E-4FFE-83EF-5A124BC914DB}']
      function Render: TgoBsonDocument;
      function ToBson: TBytes;
      function ToJson(const ASettings: TgoJsonWriterSettings): String;
    end;
  private
    FImpl: IProjection;
    class function GetEmpty: TgoMongoProjection; static; inline;
  {$ENDREGION 'Internal Declarations'}
  public
    (* Implicitly converts a Json string to a projection. Use this converter if
       you have manually created a projection Json (such as <tt>{ _id: 0 }</tt>)
       and want to use it as a filter.
       The Json string must be parseable to a BSON document.

       Raises:
         EgrJsonParserError or EInvalidOperation on parse errors *)
    class operator Implicit(const AJson: String): TgoMongoProjection; static;

    (* Implicitly converts a BSON Document to a projection. Use this converter
       if you have manually created a BSON document, and want to use it as a
       projection. *)
    class operator Implicit(const ADocument: TgoBsonDocument): TgoMongoProjection; static;

    (* Combines two projections into a single projection. *)
    class operator Add(const ALeft, ARight: TgoMongoProjection): TgoMongoProjection; static;

    (* Checks if the projection has been assigned.

       Returns:
         True if the projection hasn't been assigned yet. *)
    function IsNil: Boolean; inline;

    { Unassigns the projection (like setting an object to nil).
      IsNil will return True afterwards. }
    procedure SetNil; inline;

    (* Renders the projection to a BSON Document.

       Returns:
         The BSON Document that represents this projection. *)
    function Render: TgoBsonDocument; inline;

    (* Converts the projection to binary BSON.

       Returns:
         The BSON byte stream that represents this projection. *)
    function ToBson: TBytes; inline;

    (* Converts the projection to a JSON string.

       Returns:
         This projection in JSON format.

       @bold(Note): the projection is converted using the default writer
       settings. That is, without any pretty printing, and in Strict mode. Use
       the other overload of this function to specify output settings. *)
    function ToJson: String; overload; inline;

    (* Converts the projection to a JSON string, using specified settings.

       Parameters:
         ASettings: the output settings to use, such as pretty-printing and
           Strict vs Shell mode.

       Returns:
         This projection in JSON format. *)
    function ToJson(const ASettings: TgoJsonWriterSettings): String; overload; inline;

    (* Empty projection. This is a null-projection (IsNil returns True) *)
    class property Empty: TgoMongoProjection read GetEmpty;
  public
    (* Combines two projections into a single projection.

       Parameters:
         AProjection1: the first projection.
         AProjection2: the second projection.

       Returns:
         The combined projection.

       Example: <code>TgoMongoProjection.Combine(TgoMongoProjection.Include('x'), TgoMongoProjection.Exclude('y'))</code>
       results in <tt>{x: 1, y: 0}</tt> *)
    class function Combine(const AProjection1,
      AProjection2: TgoMongoProjection): TgoMongoProjection; overload; static;

    (* Combines multiple projections into a single projection.

       Parameters:
         AProjections: the projections to combine.

       Returns:
         The combined projection.

       Example:
       <source>TgoMongoProjection.Combine([
         TgoMongoProjection.Include('x'),
         TgoMongoProjection.Include('y'),
         TgoMongoProjection.Exclude('z')])
       </source>

       results in <tt>{x: 1, y: 1, z: 0}</tt> *)
    class function Combine(const AProjections: array of TgoMongoProjection): TgoMongoProjection; overload; static;

    (* Includes a field in a projection.

       Parameters:
         AFieldName: the name of the field to include

       Returns:
         The projection.

       Example: <code>TgoMongoProjection.Include('x')</code>
       results in <tt>{x: 1}</tt> *)
    class function Include(const AFieldName: String): TgoMongoProjection; overload; static;

    (* Includes an array of fields in a projection.

       Parameters:
         AFieldNames: the names of the fields to include

       Returns:
         The projection.

       Example: <code>TgoMongoProjection.Include(['x', 'y'])</code>
       results in <tt>{x: 1, y: 1}</tt> *)
    class function Include(const AFieldNames: array of String): TgoMongoProjection; overload; static;

    (* Excludes a field from a projection.

       Parameters:
         AFieldName: the name of the field to exclude

       Returns:
         The projection.

       Example: <code>TgoMongoProjection.Exclude('x')</code>
       results in <tt>{x: 0}</tt> *)
    class function Exclude(const AFieldName: String): TgoMongoProjection; overload; static;

    (* Excludes an array of fields from a projection.

       Parameters:
         AFieldNames: the names of the fields to exclude

       Returns:
         The projection.

       Example: <code>TgoMongoProjection.Exclude(['x', 'y'])</code>
       results in <tt>{x: 0, y: 0}</tt> *)
    class function Exclude(const AFieldNames: array of String): TgoMongoProjection; overload; static;

    (* Projects the first element in an array that matches the specified
       condition (filter).

       Parameters:
         AFieldName: the name of the field (left operand).
         AFilter: the filter specifying the conditions (right operand).

       Returns:
         The projection.

       Example: <code>TgoMongoProjection.ElemMatch('a', TgoMongoFilter.Eq('b', 1))</code>
       results in <tt>{a: {$elemMatch: { b: 1}}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/projection/elemMatch/#proj._S_elemMatch *)
    class function ElemMatch(const AFieldName: String;
      const AFilter: TgoMongoFilter): TgoMongoProjection; static;

    (* Projects the document’s score assigned during $text operation.

       Parameters:
         AFieldName: the name of the field.

       Returns:
         The projection.

       Example: <code>TgoMongoProjection.MetaTextScore('a')</code>
       results in <tt>{a: {$meta: "textScore"}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/projection/meta/#proj._S_meta *)
    class function MetaTextScore(const AFieldName: String): TgoMongoProjection; static;

    (* Limits the number of elements projected from an array.

       Parameters:
         AFieldName: the name of the field.
         ALimit: the maximum number of array elements to return. When positive,
           it selects at most the first ALimit elements. When negative, it
           selects at most the last -ALimit elements.

       Returns:
         The projection.

       Example: <code>TgoMongoProjection.Slice('a', 10)</code>
       results in <tt>{a: {$slice: 10}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/projection/slice/#proj._S_slice *)
    class function Slice(const AFieldName: String; const ALimit: Integer): TgoMongoProjection; overload; static;

    (* Limits the number of elements projected from an array.

       Parameters:
         AFieldName: the name of the field.
         ASkip: the number of items in the array to skip. When positive, it
           skips the first ASkip items. When negative, it starts with the item
           that is -ASkip from the last item.
         ALimit: the maximum number of array elements to return.

       Returns:
         The projection.

       Example: <code>TgoMongoProjection.Slice('a', 10, 20)</code>
       results in <tt>{a: {$slice: [10, 20]}}</tt>

       See https://docs.mongodb.org/manual/reference/operator/projection/slice/#proj._S_slice *)
    class function Slice(const AFieldName: String; const ASkip,
      ALimit: Integer): TgoMongoProjection; overload; static;
  end;

type
  { Direction of a sort }
  TgoMongoSortDirection = (
    { Sort in ascending order }
    Ascending,

    { Sort in descending order }
    Descending);

type
  (* A MongoDB sort modifier, used to sort the results from a MongoDB query by
     some criteria. For example, the sort modifier <tt>{ age: 1 }</tt>
     sorts the result set by age in ascending order.

     Sort modifiers can be implictly converted from JSON strings and documents,
     or they can be created using static helper functions. The following 3
     statements all have the same result:

     <source>
     MySort := '{age: 1}';
     MySort := TgoBsonDocument.Create('age', 1);
     MySort := TgoMongoSort.Ascending('age');
     </source>

     You can use the add (+) operator to combine multiple sort modifiers into
     one:

     <source>
     MySort := TgoMongoSort.Decending('age') + TgoMongoSort.Ascending('posts');
     </source>

     This results in: <tt>{age: -1, posts: 1}</tt>.
     This specifies a descending sort by age first, and then an ascending sort
     by posts (in case ages are equal). *)
  TgoMongoSort = record
  {$REGION 'Internal Declarations'}
  private type
    ISort = interface
    ['{FB526276-76F3-4F67-90C9-F09010FE8F37}']
      function Render: TgoBsonDocument;
      function ToBson: TBytes;
      function ToJson(const ASettings: TgoJsonWriterSettings): String;
    end;
  private
    FImpl: ISort;
  {$ENDREGION 'Internal Declarations'}
  public
    (* Implicitly converts a Json string to a sort modifier. Use this converter
       if you have manually created a sort modifier in Json (such as
       <tt>{age: 1}</tt>) and want to use it as a sort modifier.
       The Json string must be parseable to a BSON document.

       Raises:
         EgrJsonParserError or EInvalidOperation on parse errors *)
    class operator Implicit(const AJson: String): TgoMongoSort; static;

    (* Implicitly converts a BSON Document to a sort modifier. Use this
       converter if you have manually created a BSON document, and want to use
       it as a sort modifier. *)
    class operator Implicit(const ADocument: TgoBsonDocument): TgoMongoSort; static;

    (* Combines two sort modifiers into a single sort modifier. *)
    class operator Add(const ALeft, ARight: TgoMongoSort): TgoMongoSort; static;

    (* Checks if the sort modifier has been assigned.

       Returns:
         True if the sort modifier hasn't been assigned yet. *)
    function IsNil: Boolean; inline;

    { Unassigns the sort modifier (like setting an object to nil).
      IsNil will return True afterwards. }
    procedure SetNil; inline;

    (* Renders the sort modifier to a BSON Document.

       Returns:
         The BSON Document that represents this sort modifier. *)
    function Render: TgoBsonDocument; inline;

    (* Converts the sort modifier to binary BSON.

       Returns:
         The BSON byte stream that represents this sort modifier. *)
    function ToBson: TBytes; inline;

    (* Converts the sort modifier to a JSON string.

       Returns:
         This sort modifier in JSON format.

       @bold(Note): the sort modifier is converted using the default writer
       settings. That is, without any pretty printing, and in Strict mode. Use
       the other overload of this function to specify output settings. *)
    function ToJson: String; overload; inline;

    (* Converts the sort modifier to a JSON string, using specified settings.

       Parameters:
         ASettings: the output settings to use, such as pretty-printing and
           Strict vs Shell mode.

       Returns:
         This sort modifier in JSON format. *)
    function ToJson(const ASettings: TgoJsonWriterSettings): String; overload; inline;
  public
    (* Combines two sort modifiers into a single sort modifier.

       Parameters:
         ASort1: the first sort modifier.
         ASort2: the second sort modifier.

       Returns:
         The combined sort modifier.

       Example: <code>TgrMongoSort.Combine(TgrMongoSort.Descending('age'), TgoMongoSort.Ascending('posts'))</code>
       results in <tt>{age: -1, posts: 1}</tt> *)
    class function Combine(const ASort1, ASort2: TgoMongoSort): TgoMongoSort; overload; static;

    (* Combines multiple sort modifiers into a single sort modifier.

       Parameters:
         ASorts: the sort modifiers to combine.

       Returns:
         The combined sort modifier.

       Example:
       <source>TgrMongoSort.Combine([
         TgoMongoSort.Ascending('x'),
         TgoMongoSort.Descending('y'),
         TgoMongoSort.Ascending('z')])
       </source>

       results in <tt>{x: 1, y: -1, z: 1}</tt> *)
    class function Combine(const ASorts: array of TgoMongoSort): TgoMongoSort; overload; static;

    (* Sorts a result set by a specific field in ascending order.

       Parameters:
         AFieldName: the name of the field to sort by

       Returns:
         The sort modifier.

       Example: <code>TgrMongoSort.Ascending('x')</code>
       results in <tt>{x: 1}</tt> *)
    class function Ascending(const AFieldName: String): TgoMongoSort; static;

    (* Sorts a result set by a specific field in descending order.

       Parameters:
         AFieldName: the name of the field to sort by

       Returns:
         The sort modifier.

       Example: <code>TgrMongoSort.Descending('x')</code>
       results in <tt>{x: -1}</tt> *)
    class function Descending(const AFieldName: String): TgoMongoSort; static;

    (* Creates a descending sort on the computed relevance score of a text
       search. The name of the key should be the name of the projected relevence
       score field.

       Parameters:
         AFieldName: the name of the field.

       Returns:
         The sort modifier.

       Example: <code>TgrMongoSort.MetaTextScore('awesome')</code>
       results in <tt>{awesome: {$meta: "textScore"}}</tt> *)
    class function MetaTextScore(const AFieldName: String): TgoMongoSort; static;

  end;

type
  { Used with TgoMongoUpdate.CurrentDate }
  TgoMongoCurrentDateType = (Default, Date, Timestamp);

type
  (* A MongoDB update definition, used the specify how to update a document in
     the database. For example, {$set: {age: 42}} sets the "age" field in the
     document to 42.

     Update definitions can be implictly converted from JSON strings and
     documents, or they can be created using static helper functions. The
     following 3 statements all have the same result:

     <source>
     MyUpdate := '{$set: {age: 42}}';
     MyUpdate := TgoBsonDocument.Create('$set', TgoBsonDocument.Create('age', 42));
     MyUpdate := TgoMongoUpdate.Init.&Set('age', 42);
     </source>

     To create an update definition, you always start by calling the static
     <tt>Init</tt> function. This returns a new update definition that you can
     then define further by calling methods such as &Set, Push and Inc.

     All those methods return the update definition itself, so you can chain
     them for a fluent interface. For example, this fluent statement:

     <source>
     MyUpdate := TgoMongoUpdate.Init.&Set('a', 1).&Set('b', 2);
     </source>

     is identical to:

     <source>
     MyUpdate := TgoMongoUpdate.Init;
     MyUpdate.&Set('a', 1);
     MyUpdate.&Set('b', 2);
     </source>

     You can also write it like this for a readable, but short statement:

     <source>
     MyUpdate := TgoMongoUpdate.Init
       .&Set('a', 1)
       .&Set('b', 2);
     </source>

     In all these cases, the following JSON will be generated:

     <tt>{$set: {a: 1, b: 2}}</tt>

     As you can see, multiple <tt>&Set</tt> calls are merged into a single
     <tt>$set</tt> operator. *)
  TgoMongoUpdate = record
  public const
    { Used with PushEach }
    NO_SLICE = Integer.MaxValue;

    { Used with PushEach }
    NO_POSITION = Integer.MaxValue;
  {$REGION 'Internal Declarations'}
  private type
    IUpdate = interface
    ['{9FC6C8B5-B4BA-445F-A960-67FBDF8613F4}']
      function Render: TgoBsonDocument;
      function ToBson: TBytes;
      function ToJson(const ASettings: TgoJsonWriterSettings): String;
      function IsCombine: Boolean;
    end;
  private
    FImpl: IUpdate;
  private
    function SetOrCombine(const AUpdate: IUpdate): IUpdate;
  {$ENDREGION 'Internal Declarations'}
  public
    (* Initializes a new update definition. This is like a constructor and should
       be the first call you make before calling any other methods.

       Returns:
         A new update definition, ready to be used for chaining other methods. *)
    class function Init: TgoMongoUpdate; inline; static;

    (* Implicitly converts a Json string to an update definition. Use this
       converter if you have manually created an update definition in Json (such
       as <tt>{$set: {age: 42}}</tt>) and want to use it as an update
       definition. The Json string must be parseable to a BSON document.

       Raises:
         EgrJsonParserError or EInvalidOperation on parse errors *)
    class operator Implicit(const AJson: String): TgoMongoUpdate; static;

    (* Implicitly converts a BSON Document to an update definition. Use this
       converter if you have manually created a BSON document, and want to use
       it as an update definition. *)
    class operator Implicit(const ADocument: TgoBsonDocument): TgoMongoUpdate; static;

    (* Checks if the update definition has been assigned.

       Returns:
         True if the update definition hasn't been assigned yet. *)
    function IsNil: Boolean; inline;

    { Unassigns the update definition (like setting an object to nil).
      IsNil will return True afterwards. }
    procedure SetNil; inline;

    (* Renders the update definition to a BSON Document.

       Returns:
         The BSON Document that represents this update definition. *)
    function Render: TgoBsonDocument; inline;

    (* Converts the update definition to binary BSON.

       Returns:
         The BSON byte stream that represents this update definition. *)
    function ToBson: TBytes; inline;

    (* Converts the update definition to a JSON string.

       Returns:
         This update definition in JSON format.

       @bold(Note): the update definition is converted using the default writer
       settings. That is, without any pretty printing, and in Strict mode. Use
       the other overload of this function to specify output settings. *)
    function ToJson: String; overload; inline;

    (* Converts the update definition to a JSON string, using specified
       settings.

       Parameters:
         ASettings: the output settings to use, such as pretty-printing and
           Strict vs Shell mode.

       Returns:
         This update definition in JSON format. *)
    function ToJson(const ASettings: TgoJsonWriterSettings): String; overload; inline;
  public
    {===================================
      Fields
     ===================================}

    (* Sets the value of a field in a document.

       Parameters:
         AFieldname: the name of the field to set.
         AValue: the value to set the field to.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.&Set('a', 10)</code>
       results in <tt>{$set: {a: 10}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/set/#up._S_set *)
    function &Set(const AFieldName: String; const AValue: TgoBsonValue): TgoMongoUpdate;

    (* Sets the value of a field if an update results in an insert of a
       document. Has no effect on update operations that modify existing
       documents.

       Parameters:
         AFieldname: the name of the field to set.
         AValue: the value to set the field to.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.SetOnInsert('a', 10)</code>
       results in <tt>{$setOnInsert: {a: 10}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/setOnInsert/#up._S_setOnInsert *)
    function SetOnInsert(const AFieldName: String; const AValue: TgoBsonValue): TgoMongoUpdate;

    (* Removes the specified field from a document.

       Parameters:
         AFieldname: the name of the field to remove.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.Unset('a')</code>
       results in <tt>{$unset: {a: 1}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/set/#up._S_set *)
    function Unset(const AFieldName: String): TgoMongoUpdate;

    (* Increments the value of the field by the specified amount.

       Parameters:
         AFieldname: the name of the field to increment.
         AAmount: the amount to increment the field by.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.Inc('a', 10)</code>
       results in <tt>{$inc: {a: 10}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/inc/#up._S_inc *)
    function Inc(const AFieldName: String; const AAmount: TgoBsonValue): TgoMongoUpdate; overload;

    (* Increments the value of the field by 1.

       Parameters:
         AFieldname: the name of the field to increment.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.Inc('a')</code>
       results in <tt>{$inc: {a: 1}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/inc/#up._S_inc *)
    function Inc(const AFieldName: String): TgoMongoUpdate; overload;

    (* Multiplies the value of the field by the specified amount.

       Parameters:
         AFieldname: the name of the field to set.
         AAmount: the amount to multiply the field by.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.Mul('a', 10)</code>
       results in <tt>{$mul: {a: 10}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/mul/#up._S_mul *)
    function Mul(const AFieldName: String; const AAmount: TgoBsonValue): TgoMongoUpdate;

    (* Only updates the field if the specified value is greater than the
       existing field value.

       Parameters:
         AFieldname: the name of the field to set.
         AValue: the value to set the field to if its current value is greater
           than this value.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.Max('a', 10)</code>
       results in <tt>{$max: {a: 10}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/max/#up._S_max *)
    function Max(const AFieldName: String; const AValue: TgoBsonValue): TgoMongoUpdate;

    (* Only updates the field if the specified value is less than the
       existing field value.

       Parameters:
         AFieldname: the name of the field to set.
         AValue: the value to set the field to if its current value is less
           than this value.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.Min('a', 10)</code>
       results in <tt>{$min: {a: 10}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/min/#up._S_min *)
    function Min(const AFieldName: String; const AValue: TgoBsonValue): TgoMongoUpdate;

    (* Sets the value of a field to current date, either as a Date or a
      Timestamp.

      Parameters:
        AFieldname: the name of the field to set to the current date.
        AType: (optional) type of the field to set. By default, it sets the
          current data as a Date. You can also explicitly set to Date or
          Timestamp.

       Returns:
         This update definition, so you can use it for chaining.

      Example: <code>TgrMongoUpdate.Init.CurrentDate('a', TgoMongoCurrentDateType.Timestamp)</code>
      results in <tt>{$currentDate: {a: {$type: 'timestamp'}}}</tt>

      See https://docs.mongodb.com/manual/reference/operator/update/currentDate/#up._S_currentDate *)
    function CurrentDate(const AFieldName: String;
      const AType: TgoMongoCurrentDateType = TgoMongoCurrentDateType.Default): TgoMongoUpdate;

    (* Renames a field.

       Parameters:
         AFieldname: the name of the field to set.
         ANewName: the new name of the field.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.Rename('a', 'b')</code>
       results in <tt>{$rename: {a: "b"}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/rename/#up._S_rename *)
    function Rename(const AFieldName, ANewName: String): TgoMongoUpdate;
  public
    {===================================
      Array
     ===================================}

    (* Adds an element to an array only if it does not already exist in the set.

       Parameters:
         AFieldname: the name of array field.
         AValue: the value to add to the array.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.AddToSet('a', 1)</code>
       results in <tt>{$addToSet: {a: 1}}</tt>

       If AValue is a BSON array, then that array is added as a single value.
       For example, consider the following document:

       <tt>{ letters: ["a", "b"] }</tt>

       The following operation appends the array ["c", "d"] to the "letters"
       field:
       <code>TgrMongoUpdate.Init.AddToSet('letters', TgoBsonArray.Create('c', 'd'))</code>

       so the result becomes:

       <tt>{ letters: ["a", "b", ["c", "d"] ] }</tt>

       If you intent to add the letters "c" and "d" separately, the use
       AddToSetEach instead.

       See https://docs.mongodb.com/manual/reference/operator/update/addToSet/#up._S_addToSet *)
    function AddToSet(const AFieldName: String; const AValue: TgoBsonValue): TgoMongoUpdate;

    (* As AddToSet, but adds each element separately.

       Parameters:
         AFieldname: the name of array field.
         AValues: the values to add to the array.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.AddToSetEach('a', [1, 2])</code>
       results in <tt>{$addToSet: {a: {$each: [1, 2]}}}</tt>

       For example, consider the following document:

       <tt>{ letters: ["a", "b"] }</tt>

       The following operation appends the values "c" and "d" to the "letters"
       field:
       <code>TgrMongoUpdate.Init.AddToSetEach('letters', ['c', 'd'])</code>

       so the result becomes:

       <tt>{ letters: ["a", "b", "c", "d"] }</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/addToSet/#up._S_addToSet
       See https://docs.mongodb.com/manual/reference/operator/update/each/#up._S_each *)
    function AddToSetEach(const AFieldName: String; const AValues: array of TgoBsonValue): TgoMongoUpdate;

    (* Removes the first item of an array.

       Parameters:
         AFieldname: the name of array field.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.PopFirst('a')</code>
       results in <tt>{$pop: {a: -1}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/pop/#up._S_pop *)
    function PopFirst(const AFieldName: String): TgoMongoUpdate;

    (* Removes the last item of an array.

       Parameters:
         AFieldname: the name of array field.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.PopFirst('a')</code>
       results in <tt>{$pop: {a: 1}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/pop/#up._S_pop *)
    function PopLast(const AFieldName: String): TgoMongoUpdate;

    (* Removes all array elements that have a specific value.

       Parameters:
         AFieldname: the name of array field.
         AValue: the value to remove from the array.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.Pull('a', 1)</code>
       results in <tt>{$pull: {a: 1}}</tt>

       @bold(Note): If AValue is a TgoBsonArray, Pull removes only the elements
       in the array that match the specified AValue exactly, including order.

       @bold(Note): If AValue is a TgoBsonDocument, Pull removes only the
       elements in the array that have the exact same fields and values. The
       ordering of the fields can differ.

       See https://docs.mongodb.com/manual/reference/operator/update/pull/#up._S_pull *)
    function Pull(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoUpdate;

    (* Removes all array elements that match a specified query.

       Parameters:
         AFieldname: the name of array field.
         AFilter: the query filter.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.PullFilter('a', TgoMongoFilter.Gt('b', 1))</code>
       results in <tt>{$pull: {a: {b: {$gt: 1}}}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/pull/#up._S_pull *)
    function PullFilter(const AFieldName: String;
      const AFilter: TgoMongoFilter): TgoMongoUpdate;

    (* Removes all matching values from an array.

       Parameters:
         AFieldname: the name of array field.
         AValues: the values to remove from the array.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.PullAll('a', [1, 2])</code>
       results in <tt>{$pullAll: {a: [1, 2]}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/pullAll/#up._S_pullAll *)
    function PullAll(const AFieldName: String;
      const AValues: array of TgoBsonValue): TgoMongoUpdate;

    (* Adds an item to an array.

       Parameters:
         AFieldname: the name of array field.
         AValue: the value to add to the array.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.Push('a', 1)</code>
       results in <tt>{$push: {a: 1}}</tt>

       @bold(Note): If AValue is a TgoBsonArray, Push appends the whole array
       as a <i>single</i> element.

       See https://docs.mongodb.com/manual/reference/operator/update/push/#up._S_push *)
    function Push(const AFieldName: String;
      const AValue: TgoBsonValue): TgoMongoUpdate;

    (* As Push, but adds each item separately.

       Parameters:
         AFieldname: the name of array field.
         AValues: the values to add to the array.
         ASlice: (optional) slice modifier. If 0, the array will be emptied.
           If negative, the array is updated to contain only the last ASlice
           elements. If positive, the array is updated to contain only the first
           ASlice elements. Set the NO_SLICE (default) to disable this modifier.
         APosition: (optional) position modifier. Specifies the location in the
           array at which to insert the new elements. Set to NO_POSITION
           (default) to disable this modifier.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.PushEach('a', [1, 2], 3)</code>
       results in <tt>{$push: {a: {$each: [1, 2], $slice: 3}}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/push/#up._S_push *)
    function PushEach(const AFieldName: String;
      const AValues: array of TgoBsonValue; const ASlice: Integer = NO_SLICE;
      const APosition: Integer = NO_POSITION): TgoMongoUpdate; overload;

    (* As Push, but adds each item separately.

       Parameters:
         AFieldname: the name of array field.
         AValues: the values to add to the array.
         ASort: orders the elements of the array using this sort modifier.
         ASlice: (optional) slice modifier. If 0, the array will be emptied.
           If negative, the array is updated to contain only the last ASlice
           elements. If positive, the array is updated to contain only the first
           ASlice elements. Set the NO_SLICE (default) to disable this modifier.
         APosition: (optional) position modifier. Specifies the location in the
           array at which to insert the new elements. Set to NO_POSITION
           (default) to disable this modifier.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.PushEach('a', [1, 2], TgoMongoSort.Ascending('b')</code>
       results in <tt>{$push: {a: {$each: [1, 2], $sort: {b: 1}}}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/push/#up._S_push *)
    function PushEach(const AFieldName: String;
      const AValues: array of TgoBsonValue; const ASort: TgoMongoSort;
      const ASlice: Integer = NO_SLICE;
      const APosition: Integer = NO_POSITION): TgoMongoUpdate; overload;
  public
    {===================================
      Bitwise
     ===================================}

     (* Performs a bitwise AND update of integer values.

       Parameters:
         AFieldname: the name of the field to update.
         AValue: the operand of the bitwise AND.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.BitwiseAnd('a', 10)</code>
       results in <tt>{$bit: {a: {and: 10}}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/bit/#up._S_bit *)
    function BitwiseAnd(const AFieldName: String; const AValue: TgoBsonValue): TgoMongoUpdate;

     (* Performs a bitwise OR update of integer values.

       Parameters:
         AFieldname: the name of the field to update.
         AValue: the operand of the bitwise OR.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.BitwiseXor('a', 10)</code>
       results in <tt>{$bit: {a: {or: 10}}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/bit/#up._S_bit *)
    function BitwiseOr(const AFieldName: String; const AValue: TgoBsonValue): TgoMongoUpdate;

     (* Performs a bitwise XOR update of integer values.

       Parameters:
         AFieldname: the name of the field to update.
         AValue: the operand of the bitwise XOR.

       Returns:
         This update definition, so you can use it for chaining.

       Example: <code>TgrMongoUpdate.Init.BitwiseXor('a', 10)</code>
       results in <tt>{$bit: {a: {xor: 10}}}</tt>

       See https://docs.mongodb.com/manual/reference/operator/update/bit/#up._S_bit *)
    function BitwiseXor(const AFieldName: String; const AValue: TgoBsonValue): TgoMongoUpdate;
  end;

implementation

uses
  Grijjy.Bson.IO;

type
  TBuilder = class abstract(TInterfacedObject)
  protected
    class function SupportsWriter: Boolean; virtual;
    procedure Write(const AWriter: IgoBsonBaseWriter); virtual;
    function Build: TgoBsonDocument; virtual;
  protected
    { TgoMongoFilter.IFilter }
    { TgoMongoProjection.IProjection }
    { TgoMongoSort.ISort }
    { TgoMongoUpdate.IUpdate }
    function Render: TgoBsonDocument;
    function ToBson: TBytes;
    function ToJson(const ASettings: TgoJsonWriterSettings): String;
  end;

type
  TFilter = class abstract(TBuilder, TgoMongoFilter.IFilter)
  end;

type
  TFilterEmpty = class(TFilter)
  protected
    function Build: TgoBsonDocument; override;
  end;

type
  TFilterJson = class(TFilter)
  private
    FJson: String;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const AJson: String);
  end;

type
  TFilterBsonDocument = class(TFilter)
  private
    FDocument: TgoBsonDocument;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const ADocument: TgoBsonDocument);
  end;

type
  TFilterSimple = class(TFilter)
  private
    FFieldName: String;
    FValue: TgoBsonValue;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IgoBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String; const AValue: TgoBsonValue);
  end;

type
  TFilterOperator = class(TFilter)
  private
    FFieldName: String;
    FOperator: String;
    FValue: TgoBsonValue;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IgoBsonBaseWriter); override;
  public
    constructor Create(const AFieldName, AOperator: String; const AValue: TgoBsonValue);
  end;

type
  TFilterArrayOperator = class(TFilter)
  private
    FFieldName: String;
    FOperator: String;
    FValues: TgoBsonArray;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IgoBsonBaseWriter); override;
  public
    constructor Create(const AFieldName, AOperator: String; const AValues: TgoBsonArray);
  end;

type
  TFilterArrayIndexExists = class(TFilter)
  private
    FFieldName: String;
    FIndex: Integer;
    FExists: Boolean;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IgoBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String; const AIndex: Integer;
      const AExists: Boolean);
  end;

type
  TFilterAnd = class(TFilter)
  private
    FFilters: TArray<TgoMongoFilter.IFilter>;
  private
    class procedure AddClause(const ADocument: TgoBsonDocument;
      const AClause: TgoBsonElement); static;
    class procedure PromoteFilterToDollarForm(const ADocument: TgoBsonDocument;
      const AClause: TgoBsonElement); static;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const AFilter1, AFilter2: TgoMongoFilter); overload;
    constructor Create(const AFilters: array of TgoMongoFilter); overload;
  end;

type
  TFilterOr = class(TFilter)
  private
    FFilters: TArray<TgoMongoFilter.IFilter>;
  private
    class procedure AddClause(const AClauses: TgoBsonArray;
      const AFilter: TgoBsonDocument); static;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const AFilter1, AFilter2: TgoMongoFilter); overload;
    constructor Create(const AFilters: array of TgoMongoFilter); overload;
  end;

type
  TFilterNot = class(TFilter)
  private
    FFilter: TgoMongoFilter.IFilter;
  private
    class function NegateArbitraryFilter(const AFilter: TgoBsonDocument): TgoBsonDocument; static;
    class function NegateSingleElementFilter(const AFilter: TgoBsonDocument;
      const AElement: TgoBsonElement): TgoBsonDocument; static;
    class function NegateSingleElementTopLevelOperatorFilter(
      const AFilter: TgoBsonDocument; const AElement: TgoBsonElement): TgoBsonDocument; static;
    class function NegateSingleFieldOperatorFilter(const AFieldName: String;
      const AElement: TgoBsonElement): TgoBsonDocument; static;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const AOperand: TgoMongoFilter);
  end;

type
  TFilterElementMatch = class(TFilter)
  private
    FFieldName: String;
    FFilter: TgoMongoFilter.IFilter;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const AFieldName: String; const AFilter: TgoMongoFilter);
  end;

type
  TProjection = class abstract(TBuilder, TgoMongoProjection.IProjection)
  end;

type
  TProjectionJson = class(TProjection)
  private
    FJson: String;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const AJson: String);
  end;

type
  TProjectionBsonDocument = class(TProjection)
  private
    FDocument: TgoBsonDocument;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const ADocument: TgoBsonDocument);
  end;

type
  TProjectionSingleField = class(TProjection)
  private
    FFieldName: String;
    FValue: TgoBsonValue;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IgoBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String; const AValue: TgoBsonValue);
  end;

type
  TProjectionMultipleFields = class(TProjection)
  private
    FFieldNames: TArray<String>;
    FValue: Integer;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IgoBsonBaseWriter); override;
  public
    constructor Create(const AFieldNames: array of String;
      const AValue: Integer);
  end;

type
  TProjectionCombine = class(TProjection)
  private
    FProjections: TArray<TgoMongoProjection.IProjection>;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const AProjection1, AProjection2: TgoMongoProjection); overload;
    constructor Create(const AProjections: array of TgoMongoProjection); overload;
  end;

type
  TProjectionElementMatch = class(TProjection)
  private
    FFieldName: String;
    FFilter: TgoMongoFilter.IFilter;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const AFieldName: String; const AFilter: TgoMongoFilter);
  end;

type
  TSort = class abstract(TBuilder, TgoMongoSort.ISort)
  end;

type
  TSortJson = class(TSort)
  private
    FJson: String;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const AJson: String);
  end;

type
  TSortBsonDocument = class(TSort)
  private
    FDocument: TgoBsonDocument;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const ADocument: TgoBsonDocument);
  end;

type
  TSortCombine = class(TSort)
  private
    FSorts: TArray<TgoMongoSort.ISort>;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const ASort1, ASort2: TgoMongoSort); overload;
    constructor Create(const ASorts: array of TgoMongoSort); overload;
  end;

type
  TSortDirectional = class(TSort)
  private
    FFieldName: String;
    FDirection: TgoMongoSortDirection;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IgoBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String;
      const ADirection: TgoMongoSortDirection);
  end;

type
  TUpdate = class abstract(TBuilder, TgoMongoUpdate.IUpdate)
  protected
    { TgoMongoUpdate.IUpdate }
    function IsCombine: Boolean; virtual;
  end;

type
  TUpdateJson = class(TUpdate)
  private
    FJson: String;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const AJson: String);
  end;

type
  TUpdateBsonDocument = class(TUpdate)
  private
    FDocument: TgoBsonDocument;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const ADocument: TgoBsonDocument);
  end;

type
  TUpdateOperator = class(TUpdate)
  private
    FOperator: String;
    FFieldName: String;
    FValue: TgoBsonValue;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IgoBsonBaseWriter); override;
  public
    constructor Create(const AOperator, AFieldName: String;
      const AValue: TgoBsonValue);
  end;

type
  TUpdateBitwiseOperator = class(TUpdate)
  private
    FOperator: String;
    FFieldName: String;
    FValue: TgoBsonValue;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IgoBsonBaseWriter); override;
  public
    constructor Create(const AOperator, AFieldName: String;
      const AValue: TgoBsonValue);
  end;

type
  TUpdateAddToSet = class(TUpdate)
  private
    FFieldName: String;
    FValues: TArray<TgoBsonValue>;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IgoBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String;
      const AValue: TgoBsonValue); overload;
    constructor Create(const AFieldName: String;
      const AValues: array of TgoBsonValue); overload;
  end;

type
  TUpdatePull = class(TUpdate)
  private
    FFieldName: String;
    FFilter: TgoMongoFilter;
    FValues: TArray<TgoBsonValue>;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IgoBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String;
      const AValue: TgoBsonValue); overload;
    constructor Create(const AFieldName: String;
      const AValues: array of TgoBsonValue); overload;
    constructor Create(const AFieldName: String;
      const AFilter: TgoMongoFilter); overload;
  end;

type
  TUpdatePush = class(TUpdate)
  private
    FFieldName: String;
    FValues: TArray<TgoBsonValue>;
    FSlice: Integer;
    FPosition: Integer;
    FSort: TgoMongoSort;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IgoBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String;
      const AValue: TgoBsonValue); overload;
    constructor Create(const AFieldName: String;
      const AValues: array of TgoBsonValue; const ASlice, APosition: Integer;
      const ASort: TgoMongoSort); overload;
  end;

type
  TUpdateCombine = class(TUpdate)
  private
    FUpdates: TArray<TgoMongoUpdate.IUpdate>;
    FCount: Integer;
  protected
    { TgoMongoUpdate.IUpdate }
    function IsCombine: Boolean; override;
  protected
    function Build: TgoBsonDocument; override;
  public
    constructor Create(const AUpdate1, AUpdate2: TgoMongoUpdate.IUpdate); overload;
    constructor Create(const AUpdate1, AUpdate2: TgoMongoUpdate); overload;
    constructor Create(const AUpdates: array of TgoMongoUpdate); overload;
    procedure Add(const AUpdate: TgoMongoUpdate.IUpdate);
  end;

{ TgoMongoFilter }

class function TgoMongoFilter.All(const AFieldName: String;
  const AValues: TArray<TgoBsonValue>): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$all', TgoBsonArray.Create(AValues));
end;

class function TgoMongoFilter.All(const AFieldName: String;
  const AValues: array of TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$all', TgoBsonArray.Create(AValues));
end;

class function TgoMongoFilter.&Mod(const AFieldName: String; const ADivisor,
  ARemainder: Int64): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$mod',
    TgoBsonArray.Create([ADivisor, ARemainder]));
end;

class function TgoMongoFilter.Ne(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$ne', AValue);
end;

class function TgoMongoFilter.Nin(const AFieldName: String;
  const AValues: TArray<TgoBsonValue>): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$nin', TgoBsonArray.Create(AValues));
end;

class function TgoMongoFilter.Nin(const AFieldName: String;
  const AValues: array of TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$nin', TgoBsonArray.Create(AValues));
end;

class function TgoMongoFilter.Nin(const AFieldName: String;
  const AValues: TgoBsonArray): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$nin', AValues);
end;

class function TgoMongoFilter.&Not(
  const AOperand: TgoMongoFilter): TgoMongoFilter;
begin
  Result.FImpl := TFilterNot.Create(AOperand);
end;

class function TgoMongoFilter.&Or(const AFilter1,
  AFilter2: TgoMongoFilter): TgoMongoFilter;
begin
  Result.FImpl := TFilterOr.Create(AFilter1, AFilter2);
end;

class function TgoMongoFilter.&Or(
  const AFilters: array of TgoMongoFilter): TgoMongoFilter;
begin
  Result.FImpl := TFilterOr.Create(AFilters);
end;

class function TgoMongoFilter.&Type(const AFieldName: String;
  const AType: TgoBsonType): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$type', Ord(AType));
end;

class function TgoMongoFilter.&Type(const AFieldName,
  AType: String): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$type', AType);
end;

class function TgoMongoFilter.All(const AFieldName: String;
  const AValues: TgoBsonArray): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$all', AValues);
end;

class function TgoMongoFilter.&And(const AFilter1,
  AFilter2: TgoMongoFilter): TgoMongoFilter;
begin
  Result.FImpl := TFilterAnd.Create(AFilter1, AFilter2);
end;

class function TgoMongoFilter.&And(
  const AFilters: array of TgoMongoFilter): TgoMongoFilter;
begin
  Result.FImpl := TFilterAnd.Create(AFilters);
end;

class function TgoMongoFilter.AnyEq(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterSimple.Create(AFieldName, AValue);
end;

class function TgoMongoFilter.AnyGt(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$gt', AValue);
end;

class function TgoMongoFilter.AnyGte(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$gte', AValue);
end;

class function TgoMongoFilter.AnyLt(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$lt', AValue);
end;

class function TgoMongoFilter.AnyLte(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$lte', AValue);
end;

class function TgoMongoFilter.AnyNe(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$ne', AValue);
end;

class function TgoMongoFilter.BitsAllClear(const AFieldName: String;
  const ABitMask: UInt64): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$bitsAllClear', ABitMask);
end;

class function TgoMongoFilter.BitsAllSet(const AFieldName: String;
  const ABitMask: UInt64): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$bitsAllSet', ABitMask);
end;

class function TgoMongoFilter.BitsAnyClear(const AFieldName: String;
  const ABitMask: UInt64): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$bitsAnyClear', ABitMask);
end;

class function TgoMongoFilter.BitsAnySet(const AFieldName: String;
  const ABitMask: UInt64): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$bitsAnySet', ABitMask);
end;

class constructor TgoMongoFilter.Create;
begin
  FEmpty.FImpl := TFilterEmpty.Create;
end;

class function TgoMongoFilter.ElemMatch(const AFieldName: String;
  const AFilter: TgoMongoFilter): TgoMongoFilter;
begin
  Result.FImpl := TFilterElementMatch.Create(AFieldName, AFilter);
end;

class function TgoMongoFilter.Eq(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterSimple.Create(AFieldName, AValue);
end;

class function TgoMongoFilter.Exists(const AFieldName: String;
  const AExists: Boolean): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$exists', AExists);
end;

class function TgoMongoFilter.Gt(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$gt', AValue);
end;

class function TgoMongoFilter.Gte(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$gte', AValue);
end;

class operator TgoMongoFilter.Implicit(const AJson: String): TgoMongoFilter;
begin
  Result.FImpl := TFilterJson.Create(AJson);
end;

class operator TgoMongoFilter.Implicit(
  const ADocument: TgoBsonDocument): TgoMongoFilter;
begin
  if (ADocument.IsNil) then
    Result.FImpl := nil
  else
    Result.FImpl := TFilterBsonDocument.Create(ADocument);
end;

class function TgoMongoFilter.&In(const AFieldName: String;
  const AValues: TArray<TgoBsonValue>): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$in', TgoBsonArray.Create(AValues));
end;

class function TgoMongoFilter.&In(const AFieldName: String;
  const AValues: array of TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$in', TgoBsonArray.Create(AValues));
end;

class function TgoMongoFilter.&In(const AFieldName: String;
  const AValues: TgoBsonArray): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$in', AValues);
end;

function TgoMongoFilter.IsNil: Boolean;
begin
  Result := (FImpl = nil);
end;

class operator TgoMongoFilter.LogicalAnd(const ALeft,
  ARight: TgoMongoFilter): TgoMongoFilter;
begin
  Result.FImpl := TFilterAnd.Create(ALeft, ARight);
end;

class operator TgoMongoFilter.LogicalNot(
  const AOperand: TgoMongoFilter): TgoMongoFilter;
begin
  Result.FImpl := TFilterNot.Create(AOperand);
end;

class operator TgoMongoFilter.LogicalOr(const ALeft,
  ARight: TgoMongoFilter): TgoMongoFilter;
begin
  Result.FImpl := TFilterOr.Create(ALeft, ARight);
end;

class function TgoMongoFilter.Lt(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$lt', AValue);
end;

class function TgoMongoFilter.Lte(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$lte', AValue);
end;

class function TgoMongoFilter.Regex(const AFieldName: String;
  const ARegex: TgoBsonRegularExpression): TgoMongoFilter;
begin
  Result.FImpl := TFilterSimple.Create(AFieldName, ARegex);
end;

function TgoMongoFilter.Render: TgoBsonDocument;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.Render;
end;

procedure TgoMongoFilter.SetNil;
begin
  FImpl := nil;
end;

class function TgoMongoFilter.Size(const AFieldName: String;
  const ASize: Integer): TgoMongoFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$size', ASize);
end;

class function TgoMongoFilter.SizeGt(const AFieldName: String;
  const ASize: Integer): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayIndexExists.Create(AFieldName, ASize, True);
end;

class function TgoMongoFilter.SizeGte(const AFieldName: String;
  const ASize: Integer): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayIndexExists.Create(AFieldName, ASize - 1, True);
end;

class function TgoMongoFilter.SizeLt(const AFieldName: String;
  const ASize: Integer): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayIndexExists.Create(AFieldName, ASize - 1, False);
end;

class function TgoMongoFilter.SizeLte(const AFieldName: String;
  const ASize: Integer): TgoMongoFilter;
begin
  Result.FImpl := TFilterArrayIndexExists.Create(AFieldName, ASize, False);
end;

class function TgoMongoFilter.Text(const AText: String;
  const AOptions: TgoMongoTextSearchOptions;
  const ALanguage: String): TgoMongoFilter;
var
  Settings: TgoBsonDocument;
begin
  Settings := TgoBsonDocument.Create;
  Settings.Add('$search', AText);
  if (ALanguage <> '') then
    Settings.Add('$language', ALanguage);
  if (TgoMongoTextSearchOption.CaseSensitive in AOptions) then
    Settings.Add('$caseSensitive', True);
  if (TgoMongoTextSearchOption.DiacriticSensitive in AOptions) then
    Settings.Add('$diacriticSensitive', True);

  Result.FImpl := TFilterBsonDocument.Create(TgoBsonDocument.Create('$text', Settings));
end;

function TgoMongoFilter.ToJson: String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(TgoJsonWriterSettings.Default);
end;

function TgoMongoFilter.ToBson: TBytes;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToBson;
end;

function TgoMongoFilter.ToJson(const ASettings: TgoJsonWriterSettings): String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(ASettings);
end;

{ TgoMongoProjection }

class operator TgoMongoProjection.Implicit(const AJson: String): TgoMongoProjection;
begin
  Result.FImpl := TProjectionJson.Create(AJson);
end;

class function TgoMongoProjection.Combine(const AProjection1,
  AProjection2: TgoMongoProjection): TgoMongoProjection;
begin
  Result.FImpl := TProjectionCombine.Create(AProjection1, AProjection2);
end;

class function TgoMongoProjection.Combine(
  const AProjections: array of TgoMongoProjection): TgoMongoProjection;
begin
  Result.FImpl := TProjectionCombine.Create(AProjections);
end;

class function TgoMongoProjection.ElemMatch(const AFieldName: String;
  const AFilter: TgoMongoFilter): TgoMongoProjection;
begin
  Result.FImpl := TProjectionElementMatch.Create(AFieldName, AFilter);
end;

class function TgoMongoProjection.Exclude(
  const AFieldNames: array of String): TgoMongoProjection;
begin
  Result.FImpl := TProjectionMultipleFields.Create(AFieldNames, 0);
end;

class function TgoMongoProjection.Exclude(
  const AFieldName: String): TgoMongoProjection;
begin
  Result.FImpl := TProjectionSingleField.Create(AFieldName, 0);
end;

class function TgoMongoProjection.GetEmpty: TgoMongoProjection;
begin
  Result.FImpl := nil;
end;

class operator TgoMongoProjection.Implicit(const ADocument: TgoBsonDocument): TgoMongoProjection;
begin
  if (ADocument.IsNil) then
    Result.FImpl := nil
  else
    Result.FImpl := TProjectionBsonDocument.Create(ADocument);
end;

class operator TgoMongoProjection.Add(const ALeft, ARight: TgoMongoProjection): TgoMongoProjection;
begin
  Result.FImpl := TProjectionCombine.Create(ALeft, ARight);
end;

class function TgoMongoProjection.Include(
  const AFieldName: String): TgoMongoProjection;
begin
  Result.FImpl := TProjectionSingleField.Create(AFieldName, 1);
end;

class function TgoMongoProjection.Include(
  const AFieldNames: array of String): TgoMongoProjection;
begin
  Result.FImpl := TProjectionMultipleFields.Create(AFieldNames, 1);
end;

function TgoMongoProjection.IsNil: Boolean;
begin
  Result := (FImpl = nil);
end;

class function TgoMongoProjection.MetaTextScore(
  const AFieldName: String): TgoMongoProjection;
begin
  Result.FImpl := TProjectionSingleField.Create(AFieldName,
    TgoBsonDocument.Create('$meta', 'textScore'));
end;

function TgoMongoProjection.Render: TgoBsonDocument;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.Render;
end;

class function TgoMongoProjection.Slice(const AFieldName: String;
  const ALimit: Integer): TgoMongoProjection;
begin
  Result.FImpl := TProjectionSingleField.Create(AFieldName,
    TgoBsonDocument.Create('$slice', ALimit));
end;

procedure TgoMongoProjection.SetNil;
begin
  FImpl := nil;
end;

class function TgoMongoProjection.Slice(const AFieldName: String; const ASkip,
  ALimit: Integer): TgoMongoProjection;
begin
  Result.FImpl := TProjectionSingleField.Create(AFieldName,
    TgoBsonDocument.Create('$slice', TgoBsonArray.Create([ASkip, ALimit])));
end;

function TgoMongoProjection.ToBson: TBytes;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToBson;
end;

function TgoMongoProjection.ToJson: String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(TgoJsonWriterSettings.Default);
end;

function TgoMongoProjection.ToJson(
  const ASettings: TgoJsonWriterSettings): String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(ASettings);
end;

{ TgoMongoSort }

class operator TgoMongoSort.Add(const ALeft,
  ARight: TgoMongoSort): TgoMongoSort;
begin
  Result.FImpl := TSortCombine.Create(ALeft, ARight);
end;

class function TgoMongoSort.Ascending(const AFieldName: String): TgoMongoSort;
begin
  Result.FImpl := TSortDirectional.Create(AFieldName, TgoMongoSortDirection.Ascending);
end;

class function TgoMongoSort.Combine(
  const ASorts: array of TgoMongoSort): TgoMongoSort;
begin
  Result.FImpl := TSortCombine.Create(ASorts);
end;

class function TgoMongoSort.Descending(const AFieldName: String): TgoMongoSort;
begin
  Result.FImpl := TSortDirectional.Create(AFieldName, TgoMongoSortDirection.Descending);
end;

class function TgoMongoSort.Combine(const ASort1,
  ASort2: TgoMongoSort): TgoMongoSort;
begin
  Result.FImpl := TSortCombine.Create(ASort1, ASort2);
end;

class operator TgoMongoSort.Implicit(
  const ADocument: TgoBsonDocument): TgoMongoSort;
begin
  if (ADocument.IsNil) then
    Result.FImpl := nil
  else
    Result.FImpl := TSortBsonDocument.Create(ADocument);
end;

class operator TgoMongoSort.Implicit(const AJson: String): TgoMongoSort;
begin
  Result.FImpl := TSortJson.Create(AJson);
end;

function TgoMongoSort.IsNil: Boolean;
begin
  Result := (FImpl = nil);
end;

class function TgoMongoSort.MetaTextScore(
  const AFieldName: String): TgoMongoSort;
begin
  Result.FImpl := TSortBsonDocument.Create(
    TgoBsonDocument.Create(AFieldName,
    TgoBsonDocument.Create('$meta', 'textScore')));
end;

function TgoMongoSort.Render: TgoBsonDocument;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.Render;
end;

procedure TgoMongoSort.SetNil;
begin
  FImpl := nil;
end;

function TgoMongoSort.ToBson: TBytes;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToBson;
end;

function TgoMongoSort.ToJson: String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(TgoJsonWriterSettings.Default);
end;

function TgoMongoSort.ToJson(const ASettings: TgoJsonWriterSettings): String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(ASettings);
end;

{ TgoMongoUpdate }

function TgoMongoUpdate.&Set(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$set', AFieldName, AValue));
end;

procedure TgoMongoUpdate.SetNil;
begin
  FImpl := nil;
end;

function TgoMongoUpdate.SetOnInsert(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$setOnInsert', AFieldName, AValue));
end;

function TgoMongoUpdate.SetOrCombine(const AUpdate: IUpdate): IUpdate;
begin
  if (FImpl = nil) then
    FImpl := AUpdate
  else if (FImpl.IsCombine) then
    TUpdateCombine(FImpl).Add(AUpdate)
  else
    FImpl := TUpdateCombine.Create(FImpl, AUpdate);
  Result := FImpl;
end;

function TgoMongoUpdate.AddToSet(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateAddToSet.Create(AFieldName, AValue));
end;

function TgoMongoUpdate.AddToSetEach(const AFieldName: String;
  const AValues: array of TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateAddToSet.Create(AFieldName, AValues));
end;

function TgoMongoUpdate.BitwiseAnd(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateBitwiseOperator.Create('and', AFieldName, AValue));
end;

function TgoMongoUpdate.BitwiseOr(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateBitwiseOperator.Create('or', AFieldName, AValue));
end;

function TgoMongoUpdate.BitwiseXor(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateBitwiseOperator.Create('xor', AFieldName, AValue));
end;

function TgoMongoUpdate.CurrentDate(const AFieldName: String;
  const AType: TgoMongoCurrentDateType): TgoMongoUpdate;
var
  Value: TgoBsonValue;
begin
  case AType of
    TgoMongoCurrentDateType.Date:
      Value := TgoBsonDocument.Create('$type', 'date');

    TgoMongoCurrentDateType.Timestamp:
      Value := TgoBsonDocument.Create('$type', 'timestamp');
  else
    Value := True;
  end;

  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$currentDate', AFieldName, Value));
end;

class operator TgoMongoUpdate.Implicit(
  const ADocument: TgoBsonDocument): TgoMongoUpdate;
begin
  if (ADocument.IsNil) then
    Result.FImpl := nil
  else
    Result.FImpl := TUpdateBsonDocument.Create(ADocument);
end;

function TgoMongoUpdate.Inc(const AFieldName: String;
  const AAmount: TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$inc', AFieldName, AAmount));
end;

function TgoMongoUpdate.Inc(const AFieldName: String): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$inc', AFieldName, 1));
end;

class function TgoMongoUpdate.Init: TgoMongoUpdate;
begin
  Result.FImpl := nil;
end;

class operator TgoMongoUpdate.Implicit(const AJson: String): TgoMongoUpdate;
begin
  Result.FImpl := TUpdateJson.Create(AJson);
end;

function TgoMongoUpdate.IsNil: Boolean;
begin
  Result := (FImpl = nil);
end;

function TgoMongoUpdate.Max(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$max', AFieldName, AValue));
end;

function TgoMongoUpdate.Min(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$min', AFieldName, AValue));
end;

function TgoMongoUpdate.Mul(const AFieldName: String;
  const AAmount: TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$mul', AFieldName, AAmount));
end;

function TgoMongoUpdate.PopFirst(const AFieldName: String): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$pop', AFieldName, -1));
end;

function TgoMongoUpdate.PopLast(const AFieldName: String): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$pop', AFieldName, 1));
end;

function TgoMongoUpdate.Pull(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdatePull.Create(AFieldName, AValue));
end;

function TgoMongoUpdate.PullAll(const AFieldName: String;
  const AValues: array of TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdatePull.Create(AFieldName, AValues));
end;

function TgoMongoUpdate.PullFilter(const AFieldName: String;
  const AFilter: TgoMongoFilter): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdatePull.Create(AFieldName, AFilter));
end;

function TgoMongoUpdate.Push(const AFieldName: String;
  const AValue: TgoBsonValue): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdatePush.Create(AFieldName, AValue));
end;

function TgoMongoUpdate.PushEach(const AFieldName: String;
  const AValues: array of TgoBsonValue; const ASlice,
  APosition: Integer): TgoMongoUpdate;
var
  Sort: TgoMongoSort;
begin
  Sort.SetNil;
  Result := PushEach(AFieldName, AValues, Sort, ASlice, APosition);
end;

function TgoMongoUpdate.PushEach(const AFieldName: String;
  const AValues: array of TgoBsonValue; const ASort: TgoMongoSort; const ASlice,
  APosition: Integer): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdatePush.Create(AFieldName, AValues, ASlice, APosition, ASort));
end;

function TgoMongoUpdate.Rename(const AFieldName,
  ANewName: String): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$rename', AFieldName, ANewName));
end;

function TgoMongoUpdate.Render: TgoBsonDocument;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.Render;
end;

function TgoMongoUpdate.ToBson: TBytes;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToBson;
end;

function TgoMongoUpdate.ToJson: String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(TgoJsonWriterSettings.Default);
end;

function TgoMongoUpdate.ToJson(const ASettings: TgoJsonWriterSettings): String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(ASettings);
end;

function TgoMongoUpdate.Unset(const AFieldName: String): TgoMongoUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$unset', AFieldName, 1));
end;

{ TBuilder }

function TBuilder.Build: TgoBsonDocument;
begin
  Result := TgoBsonDocument.Create;
end;

function TBuilder.Render: TgoBsonDocument;
var
  Writer: IgoBsonDocumentWriter;
begin
  if (SupportsWriter) then
  begin
    Result := TgoBsonDocument.Create;
    Writer := TgoBsonDocumentWriter.Create(Result);
    Write(Writer);
  end
  else
    Result := Build();
end;

class function TBuilder.SupportsWriter: Boolean;
begin
  Result := False;
end;

function TBuilder.ToBson: TBytes;
var
  Writer: IgoBsonWriter;
begin
  if (SupportsWriter) then
  begin
    Writer := TgoBsonWriter.Create;
    Write(Writer);
    Result := Writer.ToBson;
  end
  else
    Result := Build().ToBson;
end;

function TBuilder.ToJson(const ASettings: TgoJsonWriterSettings): String;
var
  Writer: IgoJsonWriter;
begin
  if (SupportsWriter) then
  begin
    Writer := TgoJsonWriter.Create(ASettings);
    Write(Writer);
    Result := Writer.ToJson;
  end
  else
    Result := Build().ToJson(ASettings);
end;

procedure TBuilder.Write(const AWriter: IgoBsonBaseWriter);
begin
  { No default implementation }
end;

{ TFilterEmpty }

function TFilterEmpty.Build: TgoBsonDocument;
begin
  Result := TgoBsonDocument.Create;
end;

{ TFilterJson }

function TFilterJson.Build: TgoBsonDocument;
begin
  Result := TgoBsonDocument.Parse(FJson);
end;

constructor TFilterJson.Create(const AJson: String);
begin
  inherited Create;
  FJson := AJson;
end;

{ TFilterBsonDocument }

function TFilterBsonDocument.Build: TgoBsonDocument;
begin
  Result := FDocument;
end;

constructor TFilterBsonDocument.Create(const ADocument: TgoBsonDocument);
begin
  inherited Create;
  FDocument := ADocument;
end;

{ TFilterSimple }

constructor TFilterSimple.Create(const AFieldName: String;
  const AValue: TgoBsonValue);
begin
  inherited Create;
  FFieldName := AFieldName;
  FValue := AValue;
end;

class function TFilterSimple.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TFilterSimple.Write(const AWriter: IgoBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);
  AWriter.WriteValue(FValue);
  AWriter.WriteEndDocument;
end;

{ TFilterOperator }

constructor TFilterOperator.Create(const AFieldName, AOperator: String;
  const AValue: TgoBsonValue);
begin
  inherited Create;
  FFieldName := AFieldName;
  FOperator := AOperator;
  FValue := AValue;
end;

class function TFilterOperator.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TFilterOperator.Write(const AWriter: IgoBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);

  AWriter.WriteStartDocument;
  AWriter.WriteName(FOperator);
  AWriter.WriteValue(FValue);
  AWriter.WriteEndDocument;

  AWriter.WriteEndDocument;
end;

{ TFilterArrayOperator }

constructor TFilterArrayOperator.Create(const AFieldName, AOperator: String;
  const AValues: TgoBsonArray);
begin
  inherited Create;
  FFieldName := AFieldName;
  FOperator := AOperator;
  FValues := AValues;
end;

class function TFilterArrayOperator.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TFilterArrayOperator.Write(const AWriter: IgoBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);

  AWriter.WriteStartDocument;
  AWriter.WriteName(FOperator);
  AWriter.WriteValue(FValues);
  AWriter.WriteEndDocument;

  AWriter.WriteEndDocument;
end;

{ TFilterAnd }

class procedure TFilterAnd.AddClause(const ADocument: TgoBsonDocument;
  const AClause: TgoBsonElement);
var
  Item, Value: TgoBsonValue;
  ExistingClauseValue, ClauseValue: TgoBsonDocument;
  Element: TgoBsonElement;
  I: Integer;
begin
  if (AClause.Name = '$and') then
  begin
    { Flatten out nested $and }
    for Item in AClause.Value.AsBsonArray do
    begin
      for Element in Item.AsBsonDocument do
        AddClause(ADocument, Element);
    end;
  end
  else if (ADocument.Count = 1) and (ADocument.Elements[0].Name = '$and') then
    ADocument.Values[0].AsBsonArray.Add(TgoBsonDocument.Create(AClause))
  else if (ADocument.TryGetValue(AClause.Name, Value)) then
  begin
    if (Value.IsBsonDocument) and (AClause.Value.IsBsonDocument) then
    begin
      ClauseValue := AClause.Value.AsBsonDocument;
      ExistingClauseValue := Value.AsBsonDocument;

      for I := 0 to ExistingClauseValue.Count - 1 do
      begin
        if (ClauseValue.Contains(ExistingClauseValue.Elements[I].Name)) then
        begin
          PromoteFilterToDollarForm(ADocument, AClause);
          Exit;
        end;
      end;

      for Element in ClauseValue do
        ExistingClauseValue.Add(Element);
    end
    else
      PromoteFilterToDollarForm(ADocument, AClause);
  end
  else
    ADocument.Add(AClause);
end;

function TFilterAnd.Build: TgoBsonDocument;
var
  I, J: Integer;
  RenderedFilter: TgoBsonDocument;
begin
  Result := TgoBsonDocument.Create;
  for I := 0 to Length(FFilters) - 1 do
  begin
    RenderedFilter := FFilters[I].Render;
    for J := 0 to RenderedFilter.Count - 1 do
      AddClause(Result, RenderedFilter.Elements[J]);
  end;
end;

constructor TFilterAnd.Create(const AFilter1, AFilter2: TgoMongoFilter);
begin
  Assert(not AFilter1.IsNil);
  Assert(not AFilter2.IsNil);
  inherited Create;
  SetLength(FFilters, 2);
  FFilters[0] := AFilter1.FImpl;
  FFilters[1] := AFilter2.FImpl;
end;

constructor TFilterAnd.Create(const AFilters: array of TgoMongoFilter);
var
  I: Integer;
begin
  inherited Create;
  SetLength(FFilters, Length(AFilters));
  for I := 0 to Length(AFilters) - 1 do
  begin
    Assert(not AFilters[I].IsNil);
    FFilters[I] := AFilters[I].FImpl;
  end;
end;

class procedure TFilterAnd.PromoteFilterToDollarForm(const ADocument: TgoBsonDocument;
  const AClause: TgoBsonElement);
var
  Clauses: TgoBsonArray;
  QueryElement: TgoBsonElement;
begin
  Clauses := TgoBsonArray.Create(ADocument.Count);
  for QueryElement in ADocument do
    Clauses.Add(TgoBsonDocument.Create(QueryElement));
  Clauses.Add(TgoBsonDocument.Create(AClause));
  ADocument.Clear;
  ADocument.Add('$and', Clauses)
end;

{ TFilterOr }

class procedure TFilterOr.AddClause(const AClauses: TgoBsonArray;
  const AFilter: TgoBsonDocument);
begin
  if (AFilter.Count = 1) and (AFilter.Elements[0].Name = '$or') then
    { Flatten nested $or }
    AClauses.AddRange(AFilter.Values[0].AsBsonArray)
  else
    { We could shortcut the user's query if there are no elements in the filter,
      but I'd rather be literal and let them discover the problem on their own. }
    AClauses.Add(AFilter);
end;

function TFilterOr.Build: TgoBsonDocument;
var
  I: Integer;
  Clauses: TgoBsonArray;
  RenderedFilter: TgoBsonDocument;
begin
  Clauses := TgoBsonArray.Create;
  for I := 0 to Length(FFilters) - 1 do
  begin
    RenderedFilter := FFilters[I].Render;
    AddClause(Clauses, RenderedFilter);
  end;
  Result := TgoBsonDocument.Create('$or', Clauses);
end;

constructor TFilterOr.Create(const AFilter1, AFilter2: TgoMongoFilter);
begin
  Assert(not AFilter1.IsNil);
  Assert(not AFilter2.IsNil);
  inherited Create;
  SetLength(FFilters, 2);
  FFilters[0] := AFilter1.FImpl;
  FFilters[1] := AFilter2.FImpl;
end;

constructor TFilterOr.Create(const AFilters: array of TgoMongoFilter);
var
  I: Integer;
begin
  inherited Create;
  SetLength(FFilters, Length(AFilters));
  for I := 0 to Length(AFilters) - 1 do
  begin
    Assert(not AFilters[I].IsNil);
    FFilters[I] := AFilters[I].FImpl;
  end;
end;

{ TFilterNot }

function TFilterNot.Build: TgoBsonDocument;
var
  RenderedFilter: TgoBsonDocument;
begin
  RenderedFilter := FFilter.Render;
  if (RenderedFilter.Count = 1) then
    Result := NegateSingleElementFilter(RenderedFilter, RenderedFilter.Elements[0])
  else
    Result := NegateArbitraryFilter(RenderedFilter);
end;

constructor TFilterNot.Create(const AOperand: TgoMongoFilter);
begin
  Assert(not AOperand.IsNil);
  inherited Create;
  FFilter := AOperand.FImpl;
end;

class function TFilterNot.NegateArbitraryFilter(
  const AFilter: TgoBsonDocument): TgoBsonDocument;
begin
  // $not only works as a meta operator on a single operator so simulate Not using $nor
  Result := TgoBsonDocument.Create('$nor', TgoBsonArray.Create([AFilter]));
end;

class function TFilterNot.NegateSingleElementFilter(
  const AFilter: TgoBsonDocument;
  const AElement: TgoBsonElement): TgoBsonDocument;
var
  Selector: TgoBsonDocument;
  OperatorName: String;
begin
  if (AElement.Name.Chars[0] = '$') then
    Exit(NegateSingleElementTopLevelOperatorFilter(AFilter, AElement));

  if (AElement.Value.IsBsonDocument) then
  begin
    Selector := AElement.Value.AsBsonDocument;
    if (Selector.Count > 0) then
    begin
      OperatorName := Selector.Elements[0].Name;
      Assert(OperatorName <> '');
      if (OperatorName.Chars[0] = '$') and (OperatorName <> '$ref') then
      begin
        if (Selector.Count = 1) then
          Exit(NegateSingleFieldOperatorFilter(AElement.Name, Selector.Elements[0]))
        else
          Exit(NegateArbitraryFilter(AFilter));
      end;
    end;
  end;

  if (AElement.Value.IsBsonRegularExpression) then
    Exit(TgoBsonDocument.Create(AElement.Name, TgoBsonDocument.Create('$not', AElement.Value)));

  Result := TgoBsonDocument.Create(AElement.Name,
    TgoBsonDocument.Create('$ne', AElement.Value));
end;

class function TFilterNot.NegateSingleElementTopLevelOperatorFilter(
  const AFilter: TgoBsonDocument;
  const AElement: TgoBsonElement): TgoBsonDocument;
begin
  if (AElement.Name = '$or') then
    Result := TgoBsonDocument.Create('$nor', AElement.Value)
  else if (AElement.Name = '$nor') then
    Result := TgoBsonDocument.Create('$or', AElement.Value)
  else
    Result := NegateArbitraryFilter(AFilter);
end;

class function TFilterNot.NegateSingleFieldOperatorFilter(
  const AFieldName: String; const AElement: TgoBsonElement): TgoBsonDocument;
var
  S: String;
begin
  S := AElement.Name;
  if (S = '$exists') then
    Result := TgoBsonDocument.Create(AFieldName, TgoBsonDocument.Create('$exists', not AElement.Value.AsBoolean))
  else if (S = '$in') then
    Result := TgoBsonDocument.Create(AFieldName, TgoBsonDocument.Create('$nin', AElement.Value.AsBsonArray))
  else if (S = '$ne') or (S = '$not') then
    Result := TgoBsonDocument.Create(AFieldName, AElement.Value)
  else if (S = '$nin') then
    Result := TgoBsonDocument.Create(AFieldName, TgoBsonDocument.Create('$in', AElement.Value.AsBsonArray))
  else
    Result := TgoBsonDocument.Create(AFieldName, TgoBsonDocument.Create('$not', TgoBsonDocument.Create(AElement)));
end;

{ TFilterElementMatch }

function TFilterElementMatch.Build: TgoBsonDocument;
begin
  Result := TgoBsonDocument.Create(FFieldName,
    TgoBsonDocument.Create('$elemMatch', FFilter.Render));
end;

constructor TFilterElementMatch.Create(const AFieldName: String;
  const AFilter: TgoMongoFilter);
begin
  Assert(not AFilter.IsNil);
  inherited Create;
  FFieldName := AFieldName;
  FFilter := AFilter.FImpl;
end;

{ TFilterArrayIndexExists }

constructor TFilterArrayIndexExists.Create(const AFieldName: String;
  const AIndex: Integer; const AExists: Boolean);
begin
  inherited Create;
  FFieldName := AFieldName;
  FIndex := AIndex;
  FExists := AExists;
end;

class function TFilterArrayIndexExists.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TFilterArrayIndexExists.Write(const AWriter: IgoBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName + '.' + FIndex.ToString);
  AWriter.WriteStartDocument;
  AWriter.WriteName('$exists');
  AWriter.WriteBoolean(FExists);
  AWriter.WriteEndDocument;
  AWriter.WriteEndDocument;
end;

{ TProjectionJson }

function TProjectionJson.Build: TgoBsonDocument;
begin
  Result := TgoBsonDocument.Parse(FJson);
end;

constructor TProjectionJson.Create(const AJson: String);
begin
  inherited Create;
  FJson := AJson;
end;

{ TProjectionBsonDocument }

function TProjectionBsonDocument.Build: TgoBsonDocument;
begin
  Result := FDocument;
end;

constructor TProjectionBsonDocument.Create(const ADocument: TgoBsonDocument);
begin
  inherited Create;
  FDocument := ADocument;
end;

{ TProjectionCombine }

function TProjectionCombine.Build: TgoBsonDocument;
var
  Projection: TgoMongoProjection.IProjection;
  RenderedProjection: TgoBsonDocument;
  Element: TgoBsonElement;
begin
  Result := TgoBsonDocument.Create;
  for Projection in FProjections do
  begin
    RenderedProjection := Projection.Render;
    for Element in RenderedProjection do
    begin
      Result.Remove(Element.Name);
      Result.Add(Element)
    end;
  end;
end;

constructor TProjectionCombine.Create(const AProjection1,
  AProjection2: TgoMongoProjection);
begin
  Assert(not AProjection1.IsNil);
  Assert(not AProjection2.IsNil);
  inherited Create;
  SetLength(FProjections, 2);
  FProjections[0] := AProjection1.FImpl;
  FProjections[1] := AProjection2.FImpl;
end;

constructor TProjectionCombine.Create(
  const AProjections: array of TgoMongoProjection);
var
  I: Integer;
begin
  inherited Create;
  SetLength(FProjections, Length(AProjections));
  for I := 0 to Length(AProjections) - 1 do
  begin
    Assert(not AProjections[I].IsNil);
    FProjections[I] := AProjections[I].FImpl;
  end;
end;

{ TProjectionSingleField }

constructor TProjectionSingleField.Create(const AFieldName: String;
  const AValue: TgoBsonValue);
begin
  inherited Create;
  FFieldName := AFieldName;
  FValue := AValue;
end;

class function TProjectionSingleField.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TProjectionSingleField.Write(const AWriter: IgoBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);
  AWriter.WriteValue(FValue);
  AWriter.WriteEndDocument;
end;

{ TProjectionMultipleFields }

constructor TProjectionMultipleFields.Create(const AFieldNames: array of String;
  const AValue: Integer);
var
  I: Integer;
begin
  inherited Create;
  FValue := AValue;
  SetLength(FFieldNames, Length(AFieldNames));
  for I := 0 to Length(AFieldNames) - 1 do
    FFieldNames[I] := AFieldNames[I];
end;

class function TProjectionMultipleFields.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TProjectionMultipleFields.Write(const AWriter: IgoBsonBaseWriter);
var
  I: Integer;
begin
  AWriter.WriteStartDocument;
  for I := 0 to Length(FFieldNames) - 1 do
  begin
    AWriter.WriteName(FFieldNames[I]);
    AWriter.WriteInt32(FValue);
  end;
  AWriter.WriteEndDocument;
end;

{ TProjectionElementMatch }

function TProjectionElementMatch.Build: TgoBsonDocument;
begin
  Result := TgoBsonDocument.Create(FFieldName,
    TgoBsonDocument.Create('$elemMatch', FFilter.Render));
end;

constructor TProjectionElementMatch.Create(const AFieldName: String;
  const AFilter: TgoMongoFilter);
begin
  Assert(not AFilter.IsNil);
  inherited Create;
  FFieldName := AFieldName;
  FFilter := AFilter.FImpl;
end;

{ TSortJson }

function TSortJson.Build: TgoBsonDocument;
begin
  Result := TgoBsonDocument.Parse(FJson);
end;

constructor TSortJson.Create(const AJson: String);
begin
  inherited Create;
  FJson := AJson;
end;

{ TSortBsonDocument }

function TSortBsonDocument.Build: TgoBsonDocument;
begin
  Result := FDocument;
end;

constructor TSortBsonDocument.Create(const ADocument: TgoBsonDocument);
begin
  inherited Create;
  FDocument := ADocument;
end;

{ TSortCombine }

function TSortCombine.Build: TgoBsonDocument;
var
  Sort: TgoMongoSort.ISort;
  RenderedSort: TgoBsonDocument;
  Element: TgoBsonElement;
begin
  Result := TgoBsonDocument.Create;
  for Sort in FSorts do
  begin
    RenderedSort := Sort.Render;
    for Element in RenderedSort do
    begin
      Result.Remove(Element.Name);
      Result.Add(Element)
    end;
  end;
end;

constructor TSortCombine.Create(const ASort1, ASort2: TgoMongoSort);
begin
  Assert(not ASort1.IsNil);
  Assert(not ASort2.IsNil);
  inherited Create;
  SetLength(FSorts, 2);
  FSorts[0] := ASort1.FImpl;
  FSorts[1] := ASort2.FImpl;
end;

constructor TSortCombine.Create(const ASorts: array of TgoMongoSort);
var
  I: Integer;
begin
  inherited Create;
  SetLength(FSorts, Length(ASorts));
  for I := 0 to Length(ASorts) - 1 do
  begin
    Assert(not ASorts[I].IsNil);
    FSorts[I] := ASorts[I].FImpl;
  end;
end;

{ TSortDirectional }

constructor TSortDirectional.Create(const AFieldName: String;
  const ADirection: TgoMongoSortDirection);
begin
  inherited Create;
  FFieldName := AFieldName;
  FDirection := ADirection;
end;

class function TSortDirectional.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TSortDirectional.Write(const AWriter: IgoBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);
  case FDirection of
    TgoMongoSortDirection.Ascending:
      AWriter.WriteInt32(1);

    TgoMongoSortDirection.Descending:
      AWriter.WriteInt32(-1);
  else
    Assert(False);
  end;
  AWriter.WriteEndDocument;
end;

{ TUpdate }

function TUpdate.IsCombine: Boolean;
begin
  Result := False;
end;

{ TUpdateJson }

function TUpdateJson.Build: TgoBsonDocument;
begin
  Result := TgoBsonDocument.Parse(FJson);
end;

constructor TUpdateJson.Create(const AJson: String);
begin
  inherited Create;
  FJson := AJson;
end;

{ TUpdateBsonDocument }

function TUpdateBsonDocument.Build: TgoBsonDocument;
begin
  Result := FDocument;
end;

constructor TUpdateBsonDocument.Create(const ADocument: TgoBsonDocument);
begin
  inherited Create;
  FDocument := ADocument;
end;

{ TUpdateOperator }

constructor TUpdateOperator.Create(const AOperator, AFieldName: String;
  const AValue: TgoBsonValue);
begin
  inherited Create;
  FOperator := AOperator;
  FFieldName := AFieldName;
  FValue := AValue;
end;

class function TUpdateOperator.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TUpdateOperator.Write(const AWriter: IgoBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FOperator);

  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);
  AWriter.WriteValue(FValue);
  AWriter.WriteEndDocument;

  AWriter.WriteEndDocument;
end;

{ TUpdateBitwiseOperator }

constructor TUpdateBitwiseOperator.Create(const AOperator, AFieldName: String;
  const AValue: TgoBsonValue);
begin
  inherited Create;
  FOperator := AOperator;
  FFieldName := AFieldName;
  FValue := AValue;
end;

class function TUpdateBitwiseOperator.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TUpdateBitwiseOperator.Write(const AWriter: IgoBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName('$bit');

  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);

  AWriter.WriteStartDocument;
  AWriter.WriteName(FOperator);
  AWriter.WriteValue(FValue);
  AWriter.WriteEndDocument;

  AWriter.WriteEndDocument;

  AWriter.WriteEndDocument;
end;

{ TUpdateAddToSet }

constructor TUpdateAddToSet.Create(const AFieldName: String;
  const AValue: TgoBsonValue);
begin
  inherited Create;
  FFieldName := AFieldName;
  SetLength(FValues, 1);
  FValues[0] := AValue;
end;

constructor TUpdateAddToSet.Create(const AFieldName: String;
  const AValues: array of TgoBsonValue);
var
  I: Integer;
begin
  inherited Create;
  FFieldName := AFieldName;
  SetLength(FValues, Length(AValues));
  for I := 0 to Length(AValues) - 1 do
    FValues[I] := AValues[I];
end;

class function TUpdateAddToSet.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TUpdateAddToSet.Write(const AWriter: IgoBsonBaseWriter);
var
  I: Integer;
begin
  AWriter.WriteStartDocument;

  AWriter.WriteName('$addToSet');
  AWriter.WriteStartDocument;

  AWriter.WriteName(FFieldName);

  if (Length(FValues) = 1) then
    AWriter.WriteValue(FValues[0])
  else
  begin
    AWriter.WriteStartDocument;
    AWriter.WriteName('$each');
    AWriter.WriteStartArray;

    for I := 0 to Length(FValues) - 1 do
      AWriter.WriteValue(FValues[I]);

    AWriter.WriteEndArray;
    AWriter.WriteEndDocument;
  end;

  AWriter.WriteEndDocument;

  AWriter.WriteEndDocument;
end;

{ TUpdatePull }

constructor TUpdatePull.Create(const AFieldName: String;
  const AValue: TgoBsonValue);
begin
  inherited Create;
  FFieldName := AFieldName;
  SetLength(FValues, 1);
  FValues[0] := AValue;
end;

constructor TUpdatePull.Create(const AFieldName: String;
  const AValues: array of TgoBsonValue);
var
  I: Integer;
begin
  inherited Create;
  FFieldName := AFieldName;
  SetLength(FValues, Length(AValues));
  for I := 0 to Length(AValues) - 1 do
    FValues[I] := AValues[I];
end;

constructor TUpdatePull.Create(const AFieldName: String;
  const AFilter: TgoMongoFilter);
begin
  inherited Create;
  FFieldName := AFieldName;
  FFilter := AFilter;
end;

class function TUpdatePull.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TUpdatePull.Write(const AWriter: IgoBsonBaseWriter);
var
  RenderedFilter: TgoBsonDocument;
  I: Integer;
begin
  AWriter.WriteStartDocument;
  if (FFilter.IsNil) then
  begin
    if (Length(FValues) = 1) then
      AWriter.WriteName('$pull')
    else
      AWriter.WriteName('$pullAll');
    AWriter.WriteStartDocument;

    AWriter.WriteName(FFieldName);
    if (Length(FValues) = 1) then
      AWriter.WriteValue(FValues[0])
    else
    begin
      AWriter.WriteStartArray;
      for I := 0 to Length(FValues) - 1 do
        AWriter.WriteValue(FValues[I]);
      AWriter.WriteEndArray;
    end;

    AWriter.WriteEndDocument;
  end
  else
  begin
    RenderedFilter := FFilter.Render;

    AWriter.WriteStartDocument('$pull');

    AWriter.WriteName(FFieldName);
    AWriter.WriteValue(RenderedFilter);

    AWriter.WriteEndDocument;
  end;
  AWriter.WriteEndDocument;
end;

{ TUpdatePush }

constructor TUpdatePush.Create(const AFieldName: String;
  const AValue: TgoBsonValue);
begin
  inherited Create;
  FFieldName := AFieldName;
  SetLength(FValues, 1);
  FValues[0] := AValue;
  FSlice := TgoMongoUpdate.NO_SLICE;
  FPosition := TgoMongoUpdate.NO_POSITION;
end;

constructor TUpdatePush.Create(const AFieldName: String;
  const AValues: array of TgoBsonValue; const ASlice, APosition: Integer;
  const ASort: TgoMongoSort);
var
  I: Integer;
begin
  inherited Create;
  FFieldName := AFieldName;
  SetLength(FValues, Length(AValues));
  for I := 0 to Length(AValues) - 1 do
    FValues[I] := AValues[I];
  FSlice := ASlice;
  FPosition := APosition;
  FSort := ASort;
end;

class function TUpdatePush.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TUpdatePush.Write(const AWriter: IgoBsonBaseWriter);
var
  I: Integer;
  RenderedSort: TgoBsonDocument;
begin
  AWriter.WriteStartDocument;
  AWriter.WriteStartDocument('$push');

  AWriter.WriteName(FFieldName);
  if (FSlice = TgoMongoUpdate.NO_SLICE) and (FPosition = TgoMongoUpdate.NO_POSITION)
    and (FSort.IsNil) and (Length(FValues) = 1)
  then
    AWriter.WriteValue(FValues[0])
  else
  begin
    AWriter.WriteStartDocument;

    AWriter.WriteStartArray('$each');
    for I := 0 to Length(FValues) - 1 do
      AWriter.WriteValue(FValues[I]);
    AWriter.WriteEndArray;

    if (FSlice <> TgoMongoUpdate.NO_SLICE) then
      AWriter.WriteInt32('$slice', FSlice);

    if (FPosition <> TgoMongoUpdate.NO_POSITION) then
      AWriter.WriteInt32('$position', FPosition);

    if (not FSort.IsNil) then
    begin
      RenderedSort := FSort.Render;
      AWriter.WriteName('$sort');
      AWriter.WriteValue(RenderedSort);
    end;
    AWriter.WriteEndDocument;
  end;

  AWriter.WriteEndDocument;
  AWriter.WriteEndDocument;
end;

{ TUpdateCombine }

procedure TUpdateCombine.Add(const AUpdate: TgoMongoUpdate.IUpdate);
var
  NewCapacity: Integer;
begin
  if (FCount >= Length(FUpdates)) then
  begin
    if (FCount = 0) then
      NewCapacity := 2
    else
      NewCapacity := FCount * 2;
    SetLength(FUpdates, NewCapacity);
  end;
  FUpdates[FCount] := AUpdate;
  Inc(FCount);
end;

function TUpdateCombine.Build: TgoBsonDocument;
var
  I: Integer;
  Update: TgoMongoUpdate.IUpdate;
  RenderedUpdate: TgoBsonDocument;
  Element: TgoBsonElement;
  CurrentOperatorValue: TgoBsonValue;
begin
  Result := TgoBsonDocument.Create;
  for I := 0 to FCount - 1 do
  begin
    Update := FUpdates[I];
    RenderedUpdate := Update.Render;
    for Element in RenderedUpdate do
    begin
      if (Result.TryGetValue(Element.Name, CurrentOperatorValue)) then
        Result[Element.Name] := CurrentOperatorValue.AsBsonDocument.Merge(
          Element.Value.AsBsonDocument, True)
      else
        Result.Add(Element);
    end;
  end;
end;

constructor TUpdateCombine.Create(const AUpdate1,
  AUpdate2: TgoMongoUpdate.IUpdate);
begin
  Assert(Assigned(AUpdate1));
  Assert(Assigned(AUpdate2));
  inherited Create;
  FCount := 2;
  SetLength(FUpdates, 2);
  FUpdates[0] := AUpdate1;
  FUpdates[1] := AUpdate2;
end;

constructor TUpdateCombine.Create(const AUpdate1, AUpdate2: TgoMongoUpdate);
begin
  Assert(not AUpdate1.IsNil);
  Assert(not AUpdate2.IsNil);
  inherited Create;
  FCount := 2;
  SetLength(FUpdates, 2);
  FUpdates[0] := AUpdate1.FImpl;
  FUpdates[1] := AUpdate2.FImpl;
end;

constructor TUpdateCombine.Create(const AUpdates: array of TgoMongoUpdate);
var
  I: Integer;
begin
  inherited Create;
  FCount := Length(AUpdates);
  SetLength(FUpdates, FCount);
  for I := 0 to FCount - 1 do
  begin
    Assert(not AUpdates[I].IsNil);
    FUpdates[I] := AUpdates[I].FImpl;
  end;
end;

function TUpdateCombine.IsCombine: Boolean;
begin
  Result := True;
end;

end.
