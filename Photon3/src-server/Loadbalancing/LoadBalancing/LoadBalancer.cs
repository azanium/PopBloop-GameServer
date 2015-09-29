// --------------------------------------------------------------------------------------------------------------------
// <copyright file="LoadBalancer.cs" company="Exit Games GmbH">
//   Copyright (c) Exit Games GmbH.  All rights reserved.
// </copyright>
// <summary>
//   Defines the LoadBalancer type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Photon.LoadBalancing
{
    #region using directives

    using System.Collections.Generic;

    using ExitGames.Logging;

    #endregion

    /// <summary>
    ///   Represents a ordered collection of server instances. 
    ///   The server instances are ordered by the current workload.
    /// </summary>
    /// <typeparam name = "TServer">
    ///   The type of the server instances.
    /// </typeparam>
    public class LoadBalancer<TServer>
    {
        #region Constants and Fields

        private static readonly ILogger log = LogManager.GetCurrentClassLogger();

        private readonly int maxWorkload;

        private readonly Dictionary<TServer, int> serverList;

        /// <summary>
        /// The list of servers with currently have the lowest workload
        /// </summary>
        private readonly List<TServer> minLoadServers;

        private int minLoadserverIndex;

        private int totalWorkload;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        ///   Initializes a new instance of the <see cref = "LoadBalancer{TServer}" /> class.
        /// </summary>
        /// <param name = "maxWorkload">
        ///   The maximum workload for a server instance until it is marked as busy. Null = unlimited.
        /// </param>
        public LoadBalancer(int? maxWorkload)
            : this()
        {
            this.maxWorkload = maxWorkload.GetValueOrDefault(int.MaxValue);
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref = "LoadBalancer{TServer}" /> class.
        /// </summary>
        public LoadBalancer()
        {
            this.maxWorkload = int.MaxValue;
            this.CurrentMinWorkload = int.MaxValue;
            this.serverList = new Dictionary<TServer, int>();
            this.minLoadServers = new List<TServer>();
        }

        #endregion

        #region Properties

        /// <summary>
        ///   Gets the average workload of all server instances.
        /// </summary>
        public int AverageWorkload { get; private set; }

        /// <summary>
        /// Gets the lowest workload among the server instances.
        /// </summary>
        public int CurrentMinWorkload { get; private set; }

        /// <summary>
        /// Gets the number of server instance which have the current lowest workload.
        /// </summary>
        public int MinWorkLoadServerCount
        {
            get { return this.minLoadServers.Count; }
        }

        #endregion

        #region Public Methods

        /// <summary>
        ///   Attempts to add a server instance.
        /// </summary>
        /// <param name = "server">The server instance to add.</param>
        /// <param name = "workload">The current workload of the server instance.</param>
        /// <returns>
        ///   True if the server instance was added successfully. If the server instance already exists, 
        ///   this method returns false.
        /// </returns>
        public bool TryAddServer(TServer server, int workload)
        {
            lock (this.serverList)
            {
                // check if the server instance was already added
                if (this.serverList.ContainsKey(server))
                {
                    return false;
                }

                this.serverList.Add(server, workload);
                this.UpdateServer(server, int.MaxValue, workload);
                this.UpdateAverageWorkload(workload);

                if (log.IsDebugEnabled)
                {
                    log.DebugFormat("Added server: workload={0}, min={1}, minCount={2} ", workload, this.CurrentMinWorkload, this.minLoadServers.Count);
                }
            }

            return true;
        }

        /// <summary>
        ///   Tries to get a free server instance.
        /// </summary>
        /// <param name = "server">
        ///   When this method returns, contains the server instance with the fewest workload
        ///   or null if no server instances exists.
        /// </param>
        /// <returns>
        ///   True if a server instance with enough remaining workload is found; otherwise false.
        /// </returns>
        public bool TryGetServer(out TServer server)
        {
            int workload;
            return this.TryGetServer(out server, out workload);
        }

        /// <summary>
        ///   Tries to get a free server instance.
        /// </summary>
        /// <param name = "server">
        ///   When this method returns, contains the server instance with the fewest workload
        ///   or null if no server instances exists.
        /// </param>
        /// <param name = "workload">
        ///   The current workload of the server instance with the fewest workload or -1 if no
        ///   server instances exists.
        /// </param>
        /// <returns>
        ///   True if a server instance with enough remaining workload is found; otherwise false.
        /// </returns>
        public bool TryGetServer(out TServer server, out int workload)
        {
            lock (this.serverList)
            {
                if (this.minLoadServers.Count == 0)
                {
                    server = default(TServer);
                    workload = -1;
                    return false;
                }

                this.minLoadserverIndex++;
                if (this.minLoadserverIndex >= this.minLoadServers.Count)
                {
                    this.minLoadserverIndex = 0;
                }

                server = this.minLoadServers[this.minLoadserverIndex];
                workload = this.CurrentMinWorkload;

                return workload <= this.maxWorkload;
            }
        }

        /// <summary>
        ///   Tries to remove a server instance.
        /// </summary>
        /// <param name = "server">The server instance to remove.</param>
        /// <returns>
        ///   True if the server instance was removed successfully. 
        ///   If the server instance does not exists, this method returns false.
        /// </returns>
        public bool TryRemoveServer(TServer server)
        {
            lock (this.serverList)
            {
                int workload;
                if (this.serverList.TryGetValue(server, out workload) == false)
                {
                    return false;
                }

                this.serverList.Remove(server);

                if (workload == this.CurrentMinWorkload)
                {
                    this.minLoadServers.Remove(server);
                    if (this.minLoadServers.Count == 0)
                    {
                        this.AssignNewMinWorkloadServers();
                    }
                }

                this.UpdateAverageWorkload(-workload);

                if (log.IsDebugEnabled)
                {
                    log.DebugFormat("Removed server: workload={0}, min={1}, minCount={2} ", workload, this.CurrentMinWorkload, this.minLoadServers.Count);
                }

                return true;
            }
        }

        /// <summary>
        ///   Tries to update a server instance.
        /// </summary>
        /// <param name = "server">The server to update.</param>
        /// <param name = "newWorkload">The current workload of the server instance.</param>
        /// <returns>
        ///   True if the server instance was updated successfully. 
        ///   If the server instance does not exists, this method returns false.
        /// </returns>
        public bool TryUpdateServer(TServer server, int newWorkload)
        {
            lock (this.serverList)
            {
                int oldWorkload;
                if (this.serverList.TryGetValue(server, out oldWorkload) == false)
                {
                    return false;
                }

                if (newWorkload == oldWorkload)
                {
                    return true;
                }

                this.serverList[server] = newWorkload;
                this.UpdateServer(server, oldWorkload, newWorkload);
                this.UpdateAverageWorkload(newWorkload - oldWorkload);

                if (log.IsDebugEnabled)
                {
                    log.DebugFormat("Updated server: oldWorkload={0}, newWorkload={1}, min={2}, minCount={3} ", oldWorkload, newWorkload, this.CurrentMinWorkload, this.minLoadServers.Count);
                }

                return true;
            }
        }

        #endregion

        #region Methods

        private void UpdateServer(TServer server, int oldWorkload, int newWorkload)
        {
            if (oldWorkload == newWorkload)
            {
                return;
            }

            if (newWorkload < this.CurrentMinWorkload)
            {
                this.CurrentMinWorkload = newWorkload;

                if (newWorkload <= this.maxWorkload)
                {
                    this.minLoadServers.Clear();
                    this.minLoadServers.Add(server);
                }

                return;
            }

            if (newWorkload == this.CurrentMinWorkload)
            {
                if (this.CurrentMinWorkload <= this.maxWorkload)
                {
                    this.minLoadServers.Add(server);
                }
                
                return;
            }

            if (oldWorkload == this.CurrentMinWorkload)
            {
                this.minLoadServers.Remove(server);
                if (this.minLoadServers.Count == 0)
                {
                    this.AssignNewMinWorkloadServers();
                }
            }
        }

        private void AssignNewMinWorkloadServers()
        {
            this.CurrentMinWorkload = int.MaxValue;
            foreach (KeyValuePair<TServer, int> pair in this.serverList)
            {
                if (pair.Value < this.CurrentMinWorkload)
                {
                    this.CurrentMinWorkload = pair.Value;
                }
            }

            this.minLoadServers.Clear();
            if (this.CurrentMinWorkload > this.maxWorkload)
            {
                return;
            }

            foreach (KeyValuePair<TServer, int> pair in this.serverList)
            {
                if (pair.Value == this.CurrentMinWorkload)
                {
                    this.minLoadServers.Add(pair.Key);
                }
            }
        }

        private void UpdateAverageWorkload(int diff)
        {
            if (this.serverList.Count > 0)
            {
                this.totalWorkload += diff;
                this.AverageWorkload = this.totalWorkload / this.serverList.Count;
            }
            else
            {
                this.totalWorkload = 0;
                this.AverageWorkload = 0;
            }
        }

        #endregion
    }
}