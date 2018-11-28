namespace SharpFunky.Modeling.Messages

open SharpFunky.Modeling.DataTypes

type MessageDef = {
    dataType: RecordDataType
}

type DomainEventDef = Annotated<MessageDef>

type DomainCommandDef = Annotated<MessageDef>

type EntityDef = {
    events: DomainEventDef list
    commands: DomainCommandDef list
}
