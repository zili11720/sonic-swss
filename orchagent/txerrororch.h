#ifndef TXERROR_ORCH_H
#define TXERROR_ORCH_H

#include "orch.h"
#include "timer.h"
#include "portsorch.h"
#include <memory>
#include <vector>


class TxErrorOrch: public Orch
{
public:
    TxErrorOrch(DBConnector *configDb, DBConnector *stateDb,const std::vector<std::string> &tableNames);
    ~TxErrorOrch(void);
    void doTask(Consumer &consumer);
    void doTask(swss::SelectableTimer &timer);

    void initializePortState(const std::string &portAlias);

private:
    swss::DBConnector *m_stateDb;

    std::shared_ptr<swss::DBConnector> m_countersDb;  
    std::shared_ptr<swss::Table> m_countersTable;     
    std::shared_ptr<swss::Table> m_stateTable;    
    
    SelectableTimer* m_timer; ///should I use shared_ptr?
    ExecutableTimer* m_executor;

    static const int DEFAULT_POLL_INTERVAL_SEC = 10;
    static const int DEFAULT_THRESHOLD = 10;

    int m_pollInterval;  
    int m_threshold;


    void updateTimer(int interval); 
    void txErrorCounterCheck(const Port& port);
    void updatePortStatusNotOk(const Port& port);    
    void updatePortStatusOk(const Port& port);
};

#endif
