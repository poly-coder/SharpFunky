using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Sharpfunky.KeyValueStore.Protocols;
using static Sharpfunky.KeyValueStore.Protocols.KeyValueStore;

namespace SharpFunky.KeyValueStore.CSharp
{
    public class KeyValueStoreFromGrpc : IKeyValueStore
    {
        KeyValueStoreClient innerService;

        public KeyValueStoreFromGrpc(KeyValueStoreClient innerService)
        {
            this.innerService = innerService ?? throw new ArgumentNullException(nameof(innerService));
        }

        public async Task<GetValueResult> GetValue(GetValue request, CancellationToken cToken)
        {
            var req = new GetValueRequest();
            req.Key = request.Key;
            var res = await innerService.GetValueAsync(req, new CallOptions(cancellationToken: cToken));
            return new GetValueResult(res.Found, res.Value.ToByteArray());
        }

        public async Task<PutValueResult> PutValue(PutValue request, CancellationToken cToken)
        {
            var req = new PutValueRequest();
            req.Key = request.Key;
            req.Value = Google.Protobuf.ByteString.CopyFrom(request.Value);
            var res = await innerService.PutValueAsync(req, new CallOptions(cancellationToken: cToken));
            return new PutValueResult(res.Success);
        }

        public async Task<RemoveValueResult> RemoveValue(RemoveValue request, CancellationToken cToken)
        {
            var req = new RemoveValueRequest();
            req.Key = request.Key;
            var res = await innerService.RemoveValueAsync(req, new CallOptions(cancellationToken: cToken));
            return new RemoveValueResult(res.Success);
        }
    }
}
