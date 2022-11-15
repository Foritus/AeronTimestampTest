using System.Runtime.InteropServices;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

// Remember to run the media driver.
// java -cp aeron-all-1.40.0.jar --add-opens "java.base/sun.nio.ch=ALL-UNNAMED" --add-opens "java.base/java.util.zip=ALL-UNNAMED" -XX:+UnlockExperimentalVMOptions -XX:+TrustFinalNonStaticFields -XX:+UnlockDiagnosticVMOptions -XX:GuaranteedSafepointInterval=300000 -XX:+UseParallelGC io.aeron.driver.MediaDriver

unsafe
{
    // Change here between local UDP and IPC for testing purposes
    //var channel = "aeron:udp?endpoint=127.0.0.1:44444|term-length=32M";
    var channel = "aeron:ipc?term-length=32M";
    var streamId = 12345;

    channel += "|channel-rcv-ts-offset=reserved";

    using var client = Aeron.Connect();

    using var pub = client.AddPublication(channel, streamId);
    using var sub = client.AddSubscription(channel, streamId);

    var mostRecentTimestamp = DateTime.MinValue;
    void OnMessage(IDirectBuffer buffer, int offset, int length, Header header)
    {
        if (header.ReservedValue > 0)
        {
            mostRecentTimestamp = DateTime.UnixEpoch + TimeSpan.FromTicks(header.ReservedValue / 100);
        }
    
        var msg = MemoryMarshal.Read<Message>(new ReadOnlySpan<byte>(((byte*)buffer.BufferPointer) + offset, length));
        Console.WriteLine($"Message received: {msg.Counter} @ {mostRecentTimestamp:HH:mm:ss}");
    }

    var backingBuffer = new byte[sizeof(Message)];
    using var pubBuffer = new UnsafeBuffer(backingBuffer);

    long counter = 0;
    while (true)
    {
        var msg = new Message
        {
            Counter = counter++
        };
        MemoryMarshal.Write(backingBuffer, ref msg);
        
        pub.Offer(pubBuffer);
        sub.Poll(new FragmentAssembler(OnMessage), int.MaxValue);
    }
}

struct Message
{
    public long Counter;
}