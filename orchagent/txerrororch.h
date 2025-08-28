#ifndef TXERROR_ORCH_H
#define TXERROR_ORCH_H

#include "orch.h"
#include "timer.h"
#include "portsorch.h"
#include <memory>
#include <vector>
#include <map>
#include <string>

class TxErrorOrch: public Orch
{
public:
    TxErrorOrch(DBConnector *configDb, DBConnector *stateDb , string tableName);
    ~TxErrorOrch(void);
    void doTask(Consumer &consumer);
    void doTask(swss::SelectableTimer &timer);

private:
    std::shared_ptr<swss::DBConnector> m_countersDb;  
    std::shared_ptr<swss::Table> m_countersTable;     
    std::shared_ptr<swss::Table> m_stateTable;   
    std::shared_ptr<swss::Table> m_configTable; 
    
    SelectableTimer* m_timer; 
    ExecutableTimer* m_executor;

    static const int DEFAULT_POLL_INTERVAL_SEC = 10;
    static const int DEFAULT_THRESHOLD = 10;

    int m_pollInterval;  
    int m_threshold;
    
    // Map to track last known state of each port (OK/NOT_OK)
    std::map<sai_object_id_t, std::string> m_portStateMap;

    void InitializeMonitorConfiguration();
    void updateTimer(int interval); 
    void txErrorCountersCheck();
    void configPollInterval(int pollInterval);
    void configThreshold(int threshold);
    void updatePortStatus(const Port& port, string status);
};

#endif
