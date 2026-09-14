// device-ingest · IDeviceSource：模拟器需要的"业务事实"（反向接口）
//
// 设计方案 §4。这一段是整个抽仓里最容易做脏的地方，因此单独说明：
//
// 现状问题：主仓 net/Telemetry.cc 的内置模拟器**直接查业务表**
//   （SELECT ... FROM mission / grp / link / uav_resource，还 UPDATE mission SET progress）。
// 若把模拟器搬进模块而保留查表，模块就永远带着主仓 schema，拆不干净。
//
// 做法：把"业务 → 合成数据所需的最小事实"抽象成下面的中立结构，
// **主仓实现它，模块只消费中立结构**。模块内不出现 SQL、不出现业务类型。
//
// 边界（设计方案 §4 结论）：
//   模块只管"把设备报文收进来、解析、归一、发出去"；
//   "演示数据长什么样、任务进度怎么走"是主仓的业务模拟（BusinessSim）。
#pragma once

#include <string>
#include <vector>

namespace device_ingest {

/// 中立结构：一次"任务"在地图上的中心，模拟器据此摆设备。
struct SimMission {
    std::string id;
    std::string scenarioKey;
    double      centerLng = 0.0;
    double      centerLat = 0.0;
};

/// 中立结构：一个编组（模拟器按编组给设备命名/归属）。
struct SimGroup {
    std::string id;
    std::string name;
    int         seq = 0;
};

/// 宿主实现：把业务表翻译成上面两个中立结构（主仓里就是三条 SELECT）。
class IDeviceSource {
public:
    virtual ~IDeviceSource() = default;

    /// 当前全部任务（可按 scenario_key 推导地图中心）。
    virtual std::vector<SimMission> missions() = 0;

    /// 某任务下的编组，按 seq 升序。
    virtual std::vector<SimGroup> groups(const std::string& missionId) = 0;

    /// 设备"上报"是否也要落主仓业务表。
    ///
    /// 现状 Telemetry.cc:280 直接 UPDATE mission.progress —— 这是**业务行为**，
    /// 抽仓后必须留在主仓。返回 false（默认）= 模块不回调进度，
    /// 进度推进由主仓自己的业务逻辑负责（设计方案 §4 表最后两行）。
    virtual bool acceptProgress() { return false; }
};

}  // namespace device_ingest
