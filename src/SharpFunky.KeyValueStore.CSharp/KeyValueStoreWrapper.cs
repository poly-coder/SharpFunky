using System;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Sharpfunky.KeyValueStore.Protocols;
using static Sharpfunky.KeyValueStore.Protocols.KeyValueStore;

namespace SharpFunky.KeyValueStore.CSharp
{
    public class KeyValueStoreWrapper : KeyValueStoreBase
    {
        private IKeyValueStore innerService;

        public KeyValueStoreWrapper(IKeyValueStore innerService)
        {
            this.innerService = innerService ?? throw new ArgumentNullException(nameof(innerService));
        }

        public override async Task<GetValueResponse> GetValue(GetValueRequest request, ServerCallContext context)
        {
            var req = new GetValue(request.Key);
            var res = await innerService.GetValue(req, context.CancellationToken);
            var response = new GetValueResponse();
            response.Found = res.Found;
            response.Value = ByteString.CopyFrom(res.Value);
            return response;
        }

        public override async Task<PutValueResponse> PutValue(PutValueRequest request, ServerCallContext context)
        {
            var req = new PutValue(request.Key, request.Value.ToByteArray());
            var res = await innerService.PutValue(req, context.CancellationToken);
            var response = new PutValueResponse();
            response.Success = res.Success;
            return response;
        }

        public override async Task<RemoveValueResponse> RemoveValue(RemoveValueRequest request, ServerCallContext context)
        {
            var req = new RemoveValue(request.Key);
            var res = await innerService.RemoveValue(req, context.CancellationToken);
            var response = new RemoveValueResponse();
            response.Success = res.Success;
            return response;
        }
    }
}
