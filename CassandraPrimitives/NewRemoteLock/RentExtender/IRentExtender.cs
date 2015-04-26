﻿namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.RentExtender
{
    public interface IRentExtender
    {
        void ExtendLockRent(string lockId, string rowName, string threadId);
        void ExtendQueueRent(string lockId, string rowName, string threadId, long timestamp);
    }
}