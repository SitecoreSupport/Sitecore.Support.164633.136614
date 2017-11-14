namespace Sitecore.Support.ContentSearch.Azure.Utils
{
    using System.Security.Cryptography;
    using System.Text;

    public class PublicCloudIndexParser
    {
        public static string HashUniqueId(string input)
        {
            using (MD5 md = MD5.Create())
            {
                return GetMd5Hash(md, input);
            }
        }

        public static string GetMd5Hash(HashAlgorithm md5Hash, string input)
        {
            StringBuilder builder = new StringBuilder();
            foreach (byte num2 in md5Hash.ComputeHash(Encoding.UTF8.GetBytes(input)))
            {
                builder.Append(num2.ToString("x2"));
            }
            return builder.ToString();
        }
    }
}