using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TwitterStorm.Components
{
    public class SlidingWindowCounter<T>
    {
        #region Members
        private SlotBasedCounter<T> counter;

        private int head;
        private int tail;
        private int length;
        #endregion

        #region Constructor
        public SlidingWindowCounter(int slotLength)
        {
            if (slotLength < 2)
                throw new ArgumentException("Window length must be greater than 2.");

            this.length = slotLength;
            this.counter = new SlotBasedCounter<T>(length);
            this.head = 0;
            this.tail = NextSlot(head);
        }
        #endregion

        public Dictionary<T, long> GetWindowedCounts()
        {
            var results = counter.GetAll();
            counter.CleanUp(tail);
            Advance();
            return results;
        }

        public void Increment(T obj)
        {
            counter.Increment(obj, head);
        }

        private void Advance()
        {
            head = tail;
            tail = NextSlot(head);
        }

        private int NextSlot(int slot)
        {
            return (slot + 1) % length;
        }
    }
}
