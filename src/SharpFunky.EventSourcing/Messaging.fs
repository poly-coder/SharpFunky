namespace SharpFunky.EventSourcing.Messaging

type AggregateContext = {
    aggregateType: string
    aggregateId: string
    domain: string

}
