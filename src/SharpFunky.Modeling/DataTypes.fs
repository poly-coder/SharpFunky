namespace SharpFunky.Modeling.DataTypes

type RefDataType =
    | Inline of DataType
    | Reference of string

and DataType =
    | PrimitiveDataType of PrimitiveDataType
    | RecordDataType of RecordDataType

and PrimitiveDataType =
    | StringPrimitive = 1
    | IntegerPrimitive = 2
    | FloatPrimitive = 3
    | BooleanPrimitive = 4
    | CharPrimitive = 5
    | DateTimePrimitive = 6

and RecordDataType = {
    mixedFrom: string list
    fields: RecordDataType list
}

and RecordDataTypeField = Annotated<RefDataType>

and Annotations = Annotations of Annotation list

and Annotation = {
    type': string
    value: string
    parameters: string * string list
}

and Annotated<'a> = Annotated of name: string * annotations: Annotations * data: 'a
