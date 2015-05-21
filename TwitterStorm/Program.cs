using Microsoft.SCP;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TwitterStorm
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Count() > 0)
            {
            }
            else
            {

                System.Environment.SetEnvironmentVariable("microsoft.scp.logPrefix", "TwitterStorm-LocalTest");

                SCPRuntime.Initialize();

                //If we are not running under the local context, throw an error
                if (Context.pluginType != SCPPluginType.SCP_NET_LOCAL)
                {
                    throw new Exception(string.Format("unexpected pluginType: {0}", Context.pluginType));
                }

                //Create an instance of LocalTest
                TwitterTest tweetTest = new TwitterTest();
                //Run the tests
                tweetTest.RunTestCase();

                Console.WriteLine("Any key to exit...");
                Console.ReadLine();
            }
        }
    }
}
