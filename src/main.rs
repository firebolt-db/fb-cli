fn main() {
    let args: Vec<std::ffi::CString> = std::env::args()
        .map(|a| std::ffi::CString::new(a).unwrap())
        .collect();
    let ptrs: Vec<*const std::os::raw::c_char> = args.iter().map(|a| a.as_ptr()).collect();
    std::process::exit(fb::fb_cli_main(ptrs.len() as _, ptrs.as_ptr()));
}
