package framework.nrc

sealed trait MaterializationOption

case object MOptEliminateDomains extends MaterializationOption

case object MOptInlineLets extends MaterializationOption

object MOpts {
  def apply(eliminateDomains: Boolean = false,
            inlineLets: Boolean = false): Set[MaterializationOption] =
    (
      if (eliminateDomains) Set(MOptEliminateDomains) else Set.empty[MaterializationOption]
    ) ++
    (
      if (inlineLets) Set(MOptInlineLets) else Set.empty[MaterializationOption]
    )
}