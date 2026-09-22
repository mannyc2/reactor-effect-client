/**
 * Effect 4.0.0-rc.115 names the standard TextDecoderOptions interface from its
 * declarations even when a Node consumer intentionally omits lib.dom. Node has
 * the global TextDecoder implementation; this is the only ambient shape the
 * no-DOM pack smoke supplies after first proving the raw failure is exactly the
 * known Effect declaration.
 */
interface TextDecoderOptions {
  readonly fatal?: boolean;
  readonly ignoreBOM?: boolean;
}
