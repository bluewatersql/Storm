using Microsoft.SCP;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TwitterStorm
{
    public static class TupleHelper
    {
        public static bool IsTickTuple(this SCPTuple tuple)
        {
            if (tuple == null)
                return true;

            return tuple.GetSourceStreamId().Equals(Constants.SYSTEM_TICK_STREAM_ID);
        }
    }
}
