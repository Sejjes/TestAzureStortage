using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Windows;
using Microsoft.Win32;
using Newtonsoft.Json;

namespace WpfApp2
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        private CloudStorageAccount storageAccount;

        public CloudBlobClient BlobClient { get; set; }
        public CloudTableClient TableClient { get; set; }

        public MainWindow()
        {
        
            storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            BlobClient = storageAccount.CreateCloudBlobClient();
            TableClient = storageAccount.CreateCloudTableClient();
            
            cloudQueueClient = storageAccount.CreateCloudQueueClient();
            InitializeComponent();
        }

        public CloudQueueClient cloudQueueClient { get; set; }


        private void Upload_Click(object sender, RoutedEventArgs e)
        {
            
            var fileDialog = new OpenFileDialog();
            var result = fileDialog.ShowDialog();
            
            var file = fileDialog;

            text_sekoia.ToolTip = file.FileName;

            AddFileToContainer(file);
            AddFileInfo(file);
            Notify(file);
            if ((bool) cb_read.IsChecked)
            {
                Read();
            }
        }

        private void Read()
        {
            var queue = cloudQueueClient.GetQueueReference("memchangequeue");
            queue.CreateIfNotExists();


//            if (queue.ApproximateMessageCount!=null) //What is this?
            foreach (var cloudQueueMessage in queue.GetMessages(10, TimeSpan.FromSeconds(10)))
            {
                text_read.Text += cloudQueueMessage.AsString;
                queue.DeleteMessage(cloudQueueMessage);
            }

            var table = TableClient.GetTableReference("who");
            TableQuery<DynamicTableEntity> projectionQuery = new TableQuery<DynamicTableEntity>()
                .Where(TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Sekoia"), TableOperators.And
                    , TableQuery.GenerateFilterCondition("Who", QueryComparisons.NotEqual, "Fod")))
                .Select(new string[] { "Tel" });

            EntityResolver<string> resolver = (pk, rk, ts, props, etag) => props.ContainsKey("Tel") ? props["Tel"].StringValue : null;

            text_sekoia.Text =string.Empty;
            foreach (string projected in table.ExecuteQuery(projectionQuery, resolver, null, null))
            {
                text_sekoia.Text += projected;
            }

        }

        private void Notify(OpenFileDialog file)
        {
            var queue = cloudQueueClient.GetQueueReference("memchangequeue");
            queue.CreateIfNotExists();
            queue.AddMessage(new CloudQueueMessage(JsonConvert.SerializeObject(new {Who=text_who.Text, Organisation=text_org.Text})));
        }

        public class Member:TableEntity
        {
            public string Who { get; set; }
            public string File { get; set; }

            public Member()
            {
                
            }
            public Member(string who, string file, string tel, string organisation):base(organisation, who)
            {

                Who = who;
                File = file;
                Tel = tel;
                Organisation = organisation;
            }

            public string Organisation { get; set; }

            public string Tel { get; set; }
        }

        private void AddFileInfo(OpenFileDialog file)
        {
            var table = TableClient.GetTableReference("Who");

            table.CreateIfNotExists();

            //Only one of them 
            InsertOrReplaceAsSeveralOperation(file, table);
            InsertOrReplaceAsOneOperation(file, table);
        }

        private void InsertOrReplaceAsSeveralOperation(OpenFileDialog file, CloudTable table)
        {
            TableOperation operation = TableOperation.Retrieve<Member>(text_org.Text, text_who.Text);

            TableResult result = table.Execute(operation);

            var entity = (Member) result.Result;
            if (entity != null)
            {
                entity.File = file.FileName;
                entity.Tel = text_tel.Text;

                operation = TableOperation.Replace(entity);
            }
            else
            {
                entity = new Member(text_who.Text, file.FileName, text_tel.Text, text_org.Text);
                operation = TableOperation.Insert(entity);
            }

            var updateresult = table.Execute(operation);
        }

        private void InsertOrReplaceAsOneOperation(OpenFileDialog file, CloudTable table)
        {
            var entity = new Member(text_who.Text, file.FileName, text_tel.Text, text_org.Text);
            var operation = TableOperation.InsertOrMerge(entity);


            var result = table.Execute(operation);
        }

        private void AddFileToContainer(OpenFileDialog file)
        {
            var container = BlobClient.GetContainerReference("filecontainer");


            container.CreateIfNotExists();


            var log = container.GetAppendBlobReference("log");
            if (!log.Exists())
            {
                log.CreateOrReplace();
            }


            log.AppendText("Adding file:" + file.FileName);

            var blob = container.GetBlockBlobReference(file.FileName);


            using (FileStream fs = System.IO.File.OpenRead(file.FileName))
            {
                blob.UploadFromStream(fs);
            }


            var sb = new StringBuilder();
            foreach (var listBlobItem in container.ListBlobs(null, true))
            {
                sb.AppendLine(listBlobItem.Uri.ToString());
            }
            text_data.Text = sb.ToString();
        }

        private void cb_read_Checked(object sender, RoutedEventArgs e)
        {
            if ((bool) cb_read.IsChecked)
            {
                Read();
            }
        }
    }
}
