#include "txerrororch.h"
#include "timer.h"
#include "logger.h"
#include "schema.h"
#include "portsorch.h"
#include "sai_serialize.h"
#include "portsorch.h"
#include <tuple>
#include <vector>

extern PortsOrch *gPortsOrch;

TxErrorOrch::TxErrorOrch(DBConnector *configDb, DBConnector *stateDb,const std::vector<std::string> &tableNames) :
    Orch(configDb, tableNames),
    m_stateDb(stateDb),
    m_countersDb(new DBConnector("COUNTERS_DB", 0)),
    m_stateTable(new Table(m_stateDb, STATE_TX_ERROR_TABLE_NAME)),
    m_countersTable(new Table(m_countersDb.get(), COUNTERS_TABLE))
{

    SWSS_LOG_ENTER();

    auto interv = timespec { .tv_sec = DEFAULT_POLL_INTERVAL_SEC, .tv_nsec = 0 };
    m_timer = new SelectableTimer(interv);
    m_executor = new ExecutableTimer(m_timer, this, "TX_ERROR_POLL");
    Orch::addExecutor(m_executor);
    m_timer->start();

    m_pollInterval = DEFAULT_POLL_INTERVAL_SEC;
    m_threshold = DEFAULT_THRESHOLD;
    
}

TxErrorOrch::~TxErrorOrch(void)
{
    SWSS_LOG_ENTER();

    if (m_timer) {
        m_timer->stop();
        delete m_timer;
        m_timer = nullptr;
    }
    
    if (m_executor) {
        delete m_executor;
        m_executor = nullptr;
    }
}


// Initialize port state to OK in state table
void TxErrorOrch::initializePortState(const std::string &portAlias)
{
    SWSS_LOG_ENTER();
    
    vector<FieldValueTuple> fieldValues;
    fieldValues.emplace_back("status", "OK");
    
    m_stateTable->set(portAlias, fieldValues);
    
    SWSS_LOG_INFO("Initialized tx error state for port %s", portAlias.c_str());
}

// Handle configuration changes to tx_error_monitor table (poll_interval, threshold)
void TxErrorOrch::doTask(Consumer &consumer)
{
    SWSS_LOG_ENTER();

    SWSS_LOG_NOTICE("TX Error Consumer doTask called, queue size: %zu", consumer.m_toSync.size());

    auto it = consumer.m_toSync.begin();
    while (it != consumer.m_toSync.end())
    {
        bool erase_from_queue = true;
        try
        {
            auto tuple = it->second;
            string key = kfvKey(tuple);
            string op = kfvOp(tuple);

            if(op == SET_COMMAND)
            {
                auto data = kfvFieldsValues(tuple);
                for (const auto& fv : data)
                {
                    if (fvField(fv) == "poll_interval")   
                    {
                        int interval = std::stoi(fvValue(fv));
                        if(interval<= 0)
                        {
                            SWSS_LOG_ERROR("Invalid poll_interval value, current interval value %d", m_pollInterval);
                        }
                        else
                        {
                            m_pollInterval = interval;
                            SWSS_LOG_NOTICE("TX Error polling interval set to %d seconds", m_pollInterval);
                            updateTimer(m_pollInterval);
                        }
                    }
                    else if (fvField(fv) == "threshold")
                    {
                        int threshold = std::stoi(fvValue(fv));
                        if(threshold<= 0)
                        {
                            SWSS_LOG_ERROR("Invalid threshold value, current threshold value %d", m_threshold);
                        }
                        else
                        {
                            m_threshold = threshold;
                            SWSS_LOG_NOTICE("TX Error threshold set to %d", m_threshold);
                        }
                    }
                }
            }

            //optional - fix If a del command is addad to the cli
            else if (op == DEL_COMMAND)
            {
                m_pollInterval = DEFAULT_POLL_INTERVAL_SEC;
                m_threshold = DEFAULT_THRESHOLD;
            }

        }
        catch (const std::exception& e)
        {
            SWSS_LOG_ERROR("Exception was caught in the request parser in %s: %s", typeid(*this).name(), e.what());
        }
        catch (...)
        {
            SWSS_LOG_ERROR("Unknown exception was caught in the request parser");
        }


        if (erase_from_queue)
        {
            it = consumer.m_toSync.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void TxErrorOrch::updateTimer(int interval)
{
    SWSS_LOG_ENTER();

    m_timer->stop();

    auto interv = timespec { .tv_sec = interval, .tv_nsec = 0 };
    m_timer->setInterval(interv);
    m_timer->reset();
    m_timer->start();
}

// Timer callback: check TX error counters for all ports
void TxErrorOrch::doTask(swss::SelectableTimer &timer)
{
    SWSS_LOG_ENTER();

    for (const auto& portPair : gPortsOrch->getAllPorts())
    {
        txErrorCounterCheck(portPair.second);
    }
}

// Check TX error counter for a port and update status if threshold exceeded
void TxErrorOrch::txErrorCounterCheck(const Port& port)
{
    SWSS_LOG_ENTER();

    if(port.m_type != Port::PHY) 
        return;

    sai_object_id_t oid = port.m_port_id;

    vector<FieldValueTuple> fieldValues;

    if(!m_countersTable->get(sai_serialize_object_id(oid), fieldValues)){
        SWSS_LOG_ERROR("Failed to get tx error counters for port %s", port.m_alias.c_str());
        return;
    }

    uint64_t tx_errors = 0;
    for(const auto& fv : fieldValues)
    {
        if(fvField(fv) =="SAI_PORT_STAT_IF_OUT_ERRORS")
        {
            tx_errors = std::stoull(fvValue(fv));
            break;
        }
    }

    if(tx_errors > static_cast<uint64_t>(m_threshold))
    {
        SWSS_LOG_NOTICE("TX Error counters for port %s exceeded threshold: %ld > %d", 
            port.m_alias.c_str(), tx_errors, m_threshold);
        updatePortStatusNotOk(port);
    }

    else if(tx_errors <= static_cast<uint64_t>(m_threshold))
    {
        SWSS_LOG_NOTICE("TX Error counters for port %s is below threshold: %ld <= %d", 
            port.m_alias.c_str(), tx_errors, m_threshold);
        updatePortStatusOk(port);
    }
}

// Mark port status as NOT_OK in state table
void TxErrorOrch::updatePortStatusNotOk(const Port& port)
{
    SWSS_LOG_ENTER();

    vector<FieldValueTuple> fieldValues;
 
    fieldValues.emplace_back("status", "NOT_OK");
    m_stateTable->set(port.m_alias, fieldValues);

    SWSS_LOG_NOTICE("Marking port %s (OID: %s) as NOT_OK", port.m_alias.c_str(),
                    sai_serialize_object_id(port.m_port_id).c_str());
}

// Mark port status as OK in state table
void TxErrorOrch::updatePortStatusOk(const Port& port)
{
    SWSS_LOG_ENTER();

    vector<FieldValueTuple> fieldValues;
 
    fieldValues.emplace_back("status", "OK");
    m_stateTable->set(port.m_alias, fieldValues);

    SWSS_LOG_NOTICE("Marking port %s (OID: %s) as OK", port.m_alias.c_str(),
                    sai_serialize_object_id(port.m_port_id).c_str());
}