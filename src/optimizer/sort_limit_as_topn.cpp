#include <memory>
#include <vector>
#include "catalog/schema.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> new_children;
  for (auto &child : plan->children_) {
    new_children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  AbstractPlanNodeRef newplan = plan->CloneWithChildren(std::move(new_children));

  if (newplan->GetType() == PlanType::Limit) {
    auto &children = newplan->GetChildren();
    const auto *limit_plan = dynamic_cast<const LimitPlanNode *>(newplan.get());
    SchemaRef schema = std::make_shared<const Schema>(newplan->OutputSchema());
    if (children.size() == 1 && children[0]->GetType() == PlanType::Sort) {
      const auto *sort_plan = dynamic_cast<const SortPlanNode *>(children[0].get());
      auto orderbys = sort_plan->GetOrderBy();
      return std::make_shared<TopNPlanNode>(schema, sort_plan->GetChildAt(0), orderbys, limit_plan->GetLimit());
    }
  }

  return newplan;
}

}  // namespace bustub
