#include "txerrororch.h"
#include "timer.h"
#include "logger.h"
#include "schema.h"
#include "portsorch.h"
#include "sai_serialize.h"
#include "portsorch.h"
#include <tuple>
#include <vector>
#include <unistd.h>

extern PortsOrch *gPortsOrch;

TxErrorOrch::TxErrorOrch(DBConnector *configDb, DBConnector *stateDb, string tableName) :
    Orch(configDb,tableName),
    m_stateDb(stateDb),
    m_countersDb(new DBConnector("COUNTERS_DB", 0)),
    m_stateTable(new Table(m_stateDb, STATE_TX_ERROR_TABLE_NAME)),
    m_countersTable(new Table(m_countersDb.get(), COUNTERS_TABLE)),
    m_configTable(new Table(configDb, CFG_TX_ERROR_MONITOR_TABLE_NAME))
{
    SWSS_LOG_ENTER();

    m_pollInterval = DEFAULT_POLL_INTERVAL_SEC;
    m_threshold = DEFAULT_THRESHOLD;

    InitializeMonitorConfiguration();

    auto interv = timespec { .tv_sec = DEFAULT_POLL_INTERVAL_SEC, .tv_nsec = 0 };
    m_timer = new SelectableTimer(interv);
    m_executor = new ExecutableTimer(m_timer, this, "TX_ERROR_POLL");
    Orch::addExecutor(m_executor);
    m_timer->start();
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

// Initialize monitor configuration in config table
void TxErrorOrch::InitializeMonitorConfiguration()
{
    SWSS_LOG_ENTER();

    vector<FieldValueTuple> fieldValues;

    fieldValues.emplace_back("poll_interval", to_string(m_pollInterval));
    fieldValues.emplace_back("threshold", to_string(m_threshold));

    m_configTable->set("global", fieldValues);

    SWSS_LOG_INFO("Written default TX error monitor configuration to CONFIG_DB");
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
                        configPollInterval(std::stoi(fvValue(fv))); 
                    }
                    else if (fvField(fv) == "threshold")
                    {
                        configThreshold(std::stoi(fvValue(fv)));
                    }
                }
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

        it = consumer.m_toSync.erase(it);
    }
}

void TxErrorOrch::updateTimer(int interval)
{
    SWSS_LOG_ENTER();

    auto interv = timespec { .tv_sec = interval, .tv_nsec = 0 };
    m_timer->setInterval(interv);
    m_timer->reset();
}

// Timer callback: check TX error counters for all ports
void TxErrorOrch::doTask(swss::SelectableTimer &timer)
{
    SWSS_LOG_ENTER();
    txErrorCountersCheck();
}
  
void TxErrorOrch::txErrorCountersCheck()
{
    SWSS_LOG_ENTER();

    map<string, Port> portsList = gPortsOrch->getAllPorts();

    for (const auto& portPair : portsList)
    {
        auto port = portPair.second;

        if (port.m_type != Port::PHY) 
            continue;

        sai_object_id_t oid = port.m_port_id;

        vector<FieldValueTuple> fieldValues;

        if (!m_countersTable->get(sai_serialize_object_id(oid), fieldValues))
        {
            SWSS_LOG_ERROR("Failed to get tx error counters for port %s", port.m_alias.c_str());
            continue;
        }

        uint64_t tx_errors = 0;
        for (const auto& fv : fieldValues)
        {
            if (fvField(fv) == "SAI_PORT_STAT_IF_OUT_ERRORS")
            {
                tx_errors = std::stoull(fvValue(fv));
                break;
            }
        }

        if (tx_errors > static_cast<uint64_t>(m_threshold))
        {
            SWSS_LOG_DEBUG("TX Error counters for port %s exceeded threshold: %ld > %d", 
                port.m_alias.c_str(), tx_errors, m_threshold);
            updatePortStatus(port, "NOT_OK");
        }
        else if (tx_errors <= static_cast<uint64_t>(m_threshold))
        {
            SWSS_LOG_DEBUG("TX Error counters for port %s is below threshold: %ld <= %d", 
                port.m_alias.c_str(), tx_errors, m_threshold);
            updatePortStatus(port, "OK");
        }
    }
}

void TxErrorOrch::configPollInterval(int pollInterval)
{
    SWSS_LOG_ENTER();

    if(pollInterval<= 0)
    {
        SWSS_LOG_ERROR("Invalid poll_interval value, current interval value %d", m_pollInterval);
    }
    else
    {
        m_pollInterval = pollInterval;
        SWSS_LOG_NOTICE("TX Error polling interval set to %d seconds", m_pollInterval);
        updateTimer(m_pollInterval);
    }
}
  
void TxErrorOrch::configThreshold(int threshold)
{
    SWSS_LOG_ENTER();

    if(threshold<= 0)
    {
        SWSS_LOG_ERROR("Invalid threshold value, current threshold value %d", m_threshold);
    }
    else
    {
        m_threshold = threshold;
        SWSS_LOG_NOTICE("TX Error threshold set to %d", m_threshold);
        txErrorCountersCheck();
    }
}

void TxErrorOrch::updatePortStatus(const Port& port, string status)
{
    SWSS_LOG_ENTER();

    vector<FieldValueTuple> fieldValues;
    if(status == "OK")
    {
        fieldValues.emplace_back("status", "OK");
    }
    else if(status == "NOT_OK")
    {
        fieldValues.emplace_back("status", "NOT_OK");
    }
    else
    {
        SWSS_LOG_ERROR("Invalid status value, current status value %s", status.c_str());
        return;
    }
    m_stateTable->set(port.m_alias, fieldValues);
    SWSS_LOG_NOTICE("Updated port %s status to %s", port.m_alias.c_str(), status.c_str());
}
