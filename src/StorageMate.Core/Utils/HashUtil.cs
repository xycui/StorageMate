namespace StorageMate.Core.Utils
{
    using System.Text;

    public static class HashUtil
    {
        public static string ComputeMd5Hash(string rawText)
        {
            using (var md5 = System.Security.Cryptography.MD5.Create())
            {
                byte[] inputBytes = Encoding.ASCII.GetBytes(rawText);
                byte[] hashBytes = md5.ComputeHash(inputBytes);

                // Convert the byte array to hexadecimal string
                StringBuilder sb = new StringBuilder();
                foreach (var t in hashBytes)
                {
                    sb.Append(t.ToString("X2"));
                }

                return sb.ToString();
            }
        }
    }
}
