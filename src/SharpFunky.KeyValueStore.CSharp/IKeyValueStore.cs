using System.Threading;
using System.Threading.Tasks;

namespace SharpFunky.KeyValueStore.CSharp
{

    public class GetValue
    {
        public GetValue(string key)
        {
            Key = key;
        }
        public string Key { get; }
    }

    public class GetValueResult
    {
        public GetValueResult(bool found, byte[] value)
        {
            Found = found;
            Value = value;
        }
        public bool Found { get; }
        public byte[] Value { get; }
    }

    public class PutValue
    {
        public PutValue(string key, byte[] value)
        {
            Key = key;
            Value = value;
        }
        public string Key { get; }
        public byte[] Value { get; }
    }

    public class PutValueResult
    {
        public PutValueResult(bool success)
        {
            Success = success;
        }
        public bool Success { get; }
    }

    public class RemoveValue
    {
        public RemoveValue(string key)
        {
            Key = key;
        }
        public string Key { get; }
    }

    public class RemoveValueResult
    {
        public RemoveValueResult(bool success)
        {
            Success = success;
        }
        public bool Success { get; }
    }

    public interface IKeyValueStore
    {
        Task<GetValueResult> GetValue(GetValue request, CancellationToken cToken);
        Task<PutValueResult> PutValue(PutValue request, CancellationToken cToken);
        Task<RemoveValueResult> RemoveValue(RemoveValue request, CancellationToken cToken);
    }
}
