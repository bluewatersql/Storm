using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TwitterStorm.Components
{
    public class NGram
    {
        public static IEnumerable<string> Generate(string text, int nGramSize)
        {
            if (nGramSize == 0) throw new ArgumentException("nGram size must be greater than zero");

            StringBuilder nGram = new StringBuilder();
            Queue<int> wordLengths = new Queue<int>();

            int wordCount = 0;
            int lastWordLen = 0;

            //append the first character, if valid.
            //avoids if statement for each for loop to check i==0 for before and after vars.
            if (text != "" && char.IsLetterOrDigit(text[0]))
            {
                nGram.Append(text[0]);
                lastWordLen++;
            }

            //generate ngrams
            for (int i = 1; i < text.Length - 1; i++)
            {
                char before = text[i - 1];
                char after = text[i + 1];

                if (char.IsLetterOrDigit(text[i])
                        ||
                    //keep all punctuation that is surrounded by letters or numbers on both sides.
                        (text[i] != ' '
                        && (char.IsSeparator(text[i]) || char.IsPunctuation(text[i]))
                        && (char.IsLetterOrDigit(before) && char.IsLetterOrDigit(after))
                        )
                    )
                {
                    nGram.Append(text[i]);
                    lastWordLen++;
                }
                else
                {
                    if (lastWordLen > 0)
                    {
                        wordLengths.Enqueue(lastWordLen);
                        lastWordLen = 0;
                        wordCount++;

                        if (wordCount >= nGramSize)
                        {
                            yield return nGram.ToString();
                            nGram.Remove(0, wordLengths.Dequeue() + 1);
                            wordCount -= 1;
                        }

                        nGram.Append(" ");
                    }
                }
            }
            nGram.Append(text.Last());
            yield return nGram.ToString();
        }
    }
}
