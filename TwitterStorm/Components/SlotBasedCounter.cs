using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TwitterStorm.Components
{
    public class SlotBasedCounter<T>
    {
        #region Members
        private Dictionary<T, long[]> objectCounts = new Dictionary<T, long[]>();
        private int slots;
        #endregion

        #region Constructor
        public SlotBasedCounter(int numSlots)
        {
            if (numSlots < 0)
                throw new ArgumentNullException("Number of slots must be greater than 0.");

            this.slots = numSlots;
        }
        #endregion

        public void Increment(T obj, int slot)
        {
            if (objectCounts.ContainsKey(obj) == false)
            {
                objectCounts.Add(obj, new long[this.slots]);
            }

            var counts = objectCounts[obj];
            counts[slot]++;
        }

        public void Reset(T obj, int slot)
        {
             var counts = objectCounts[obj];
                counts[slot] = 0;
        }

        public Dictionary<T, long> GetAll()
        {
            var results = new Dictionary<T, long>();

            foreach (var key in objectCounts.Keys)
            {
                results.Add(key, objectCounts[key].Sum());
            }

            return results;
        }

        public void CleanUp(int slot)
        {
            foreach (var obj in objectCounts.Keys.ToList())
            {
                Reset(obj, slot);

                if (SumTotal(obj) == 0)
                    objectCounts.Remove(obj);
            }
        }

        private long SumTotal(T obj)
        {
            long count = 0;

            if (objectCounts.ContainsKey(obj))
            {
                var counts = objectCounts[obj];
                count = counts.Sum();
            }

            return count;
        }
    }
}
