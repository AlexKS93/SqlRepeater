using System;
using System.ServiceProcess;
using System.Threading;



namespace SiknSqlRepeater
{
    public partial class Service1 : ServiceBase
    {
        Process process;
        public Service1()
        {
            InitializeComponent();
            this.CanStop = true;
            this.AutoLog = true;
        }

        protected override void OnStart(string[] args)
        {
            process = new Process();
            Thread loggerThread = new Thread(new ThreadStart(process.Start));
            loggerThread.Start();
        }

        protected override void OnStop()
        {
            process.Stop();
            Thread.Sleep(1000);
        }
    }

    
}